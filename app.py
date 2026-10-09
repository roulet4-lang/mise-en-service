import json
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Stockage
#   - DATABASE_URL défini  -> PostgreSQL (ex. Neon) : les données survivent
#                             aux redémarrages / mises en veille de Render
#   - sinon                -> SQLite local (développement) : DB_PATH ou services.db
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
SQLITE_PATH = os.getenv("DB_PATH", "services.db")


class SqliteStore:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    def _conn(self):
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init(self):
        conn = self._conn()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_services (
                    date_str TEXT PRIMARY KEY,
                    shifts_json TEXT,
                    repos_text TEXT,
                    changes_text TEXT
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def close(self):
        pass

    def get_day(self, date: str) -> dict:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM daily_services WHERE date_str = ?", (date,)
            ).fetchone()
            if row:
                return {
                    "shifts": json.loads(row["shifts_json"]),
                    "repos": row["repos_text"],
                    "changes": row["changes_text"],
                }
            return {"shifts": {}, "repos": "", "changes": ""}
        finally:
            conn.close()

    def patch(self, date: str, field: str, value: str):
        conn = self._conn()
        try:
            # Verrou d'écriture : lecture + fusion + écriture atomiques
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM daily_services WHERE date_str = ?", (date,)
            ).fetchone()
            shifts = json.loads(row["shifts_json"]) if row else {}
            repos = row["repos_text"] if row else ""
            changes = row["changes_text"] if row else ""

            if field == "repos":
                repos = value
            elif field == "changes":
                changes = value
            elif value.strip():
                shifts[field] = value.strip()
            else:
                shifts.pop(field, None)

            conn.execute("""
                INSERT INTO daily_services
                    (date_str, shifts_json, repos_text, changes_text)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(date_str) DO UPDATE SET
                    shifts_json = excluded.shifts_json,
                    repos_text = excluded.repos_text,
                    changes_text = excluded.changes_text
            """, (date, json.dumps(shifts), repos, changes))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


class PostgresStore:
    def __init__(self, url: str):
        from psycopg_pool import ConnectionPool

        self.pool = ConnectionPool(
            url,
            min_size=1,
            max_size=5,
            open=False,
            check=ConnectionPool.check_connection,  # écarte les connexions coupées
            max_idle=240,
            timeout=30,
            # prepare_threshold=None : compatible avec le pooler Neon (pgbouncer)
            kwargs={"prepare_threshold": None, "connect_timeout": 20},
        )

    def init(self):
        self.pool.open(wait=True, timeout=60)
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_services (
                    date_str TEXT PRIMARY KEY,
                    shifts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    repos_text TEXT NOT NULL DEFAULT '',
                    changes_text TEXT NOT NULL DEFAULT ''
                )
            """)

    def close(self):
        self.pool.close()

    def get_day(self, date: str) -> dict:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT shifts_json, repos_text, changes_text "
                "FROM daily_services WHERE date_str = %s",
                (date,),
            ).fetchone()
        if row:
            return {"shifts": row[0], "repos": row[1], "changes": row[2]}
        return {"shifts": {}, "repos": "", "changes": ""}

    def patch(self, date: str, field: str, value: str):
        # Chaque modification est UNE requête atomique : pas de lecture-puis-écriture,
        # donc deux collègues qui modifient des cases différentes ne s'écrasent jamais.
        with self.pool.connection() as conn:
            if field in ("repos", "changes"):
                col = "repos_text" if field == "repos" else "changes_text"
                conn.execute(
                    f"INSERT INTO daily_services (date_str, {col}) VALUES (%s, %s) "
                    f"ON CONFLICT (date_str) DO UPDATE SET {col} = EXCLUDED.{col}",
                    (date, value),
                )
            elif value.strip():
                v = value.strip()
                conn.execute(
                    "INSERT INTO daily_services (date_str, shifts_json) "
                    "VALUES (%s, jsonb_build_object(%s::text, %s::text)) "
                    "ON CONFLICT (date_str) DO UPDATE SET shifts_json = "
                    "daily_services.shifts_json || jsonb_build_object(%s::text, %s::text)",
                    (date, field, v, field, v),
                )
            else:
                conn.execute(
                    "INSERT INTO daily_services (date_str) VALUES (%s) "
                    "ON CONFLICT (date_str) DO UPDATE SET shifts_json = "
                    "daily_services.shifts_json - %s::text",
                    (date, field),
                )


store = PostgresStore(DATABASE_URL) if DATABASE_URL else SqliteStore(SQLITE_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(store.init)
    yield
    await run_in_threadpool(store.close)


app = FastAPI(lifespan=lifespan)

SLOT_RE = re.compile(r"^[A-Z][0-9]{1,2}$")


class FieldPatch(BaseModel):
    """Modification d'UN seul champ (une case, 'repos' ou 'changes')."""
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    field: str
    value: str = Field(default="", max_length=2000)

    @field_validator("field")
    @classmethod
    def check_field(cls, v):
        if v in ("repos", "changes") or SLOT_RE.match(v):
            return v
        raise ValueError("champ inconnu")


class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        for connection in list(self.active):
            try:
                await connection.send_text(json.dumps(message))
            except Exception:
                self.disconnect(connection)


manager = ConnectionManager()


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/api/day/{date_str}")
def get_day(date_str: str):
    return store.get_day(date_str)


@app.post("/api/patch")
async def patch_field(p: FieldPatch):
    await run_in_threadpool(store.patch, p.date, p.field, p.value)
    # On ne diffuse que le champ modifié
    await manager.broadcast({"date": p.date, "field": p.field, "value": p.value})
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def serve_index():
    return """<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Service Contrôle - Gestion</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>

<body class="bg-slate-100 text-slate-800 p-3 sm:p-6 pb-20">
  <div class="max-w-4xl mx-auto space-y-4">

    <!-- En-tête -->
    <div class="bg-white p-4 rounded-xl shadow-sm border border-slate-200 flex flex-wrap justify-between items-center gap-3">
      <div>
        <h1 class="text-lg font-bold text-slate-900">Service Contrôle</h1>
        <p class="text-xs text-slate-500">Mise à jour en temps réel</p>
      </div>

      <div class="flex items-center gap-3">
        <input type="date" id="selectedDate"
          class="border rounded-lg px-3 py-1.5 font-medium bg-slate-50 border-slate-300 text-sm">

        <span id="badge"
          class="px-2 py-1 text-xs rounded-full bg-emerald-100 text-emerald-700 font-semibold">
          Connecté
        </span>
      </div>
    </div>

    <!-- Postes de service -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">

      <!-- Matin -->
      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">
        <h2 class="font-bold text-amber-700 text-sm border-b pb-2 mb-3">
          Postes Matin
        </h2>
        <div id="morningInputs" class="space-y-2"></div>
      </div>

      <!-- Après-midi -->
      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">
        <h2 class="font-bold text-indigo-700 text-sm border-b pb-2 mb-3">
          Postes Après-midi
        </h2>
        <div id="afternoonInputs" class="space-y-2"></div>
      </div>

    </div>

    <!-- Repos et changements -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">

      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">
        <h2 class="font-bold text-slate-700 text-sm border-b pb-2 mb-2">
          Agents en Repos
        </h2>

        <textarea id="repos" rows="4"
          placeholder="Un nom par ligne..."
          class="w-full border border-slate-300 rounded-lg p-2 text-sm outline-none focus:border-blue-500"></textarea>
      </div>

      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">
        <h2 class="font-bold text-rose-700 text-sm border-b pb-2 mb-2">
          Changements de service
        </h2>

        <textarea id="changes" rows="4"
          placeholder="Ex: COSENTINO E4 -> MAELLE P6"
          class="w-full border border-slate-300 rounded-lg p-2 text-sm outline-none focus:border-blue-500"></textarea>
      </div>

    </div>

    <!-- Disclaimer -->
    <footer class="mt-6 rounded-xl border border-slate-200 bg-white p-4 text-center text-xs text-slate-500 shadow-sm">
      <p class="font-semibold text-slate-700">
        Information et responsabilité
      </p>

      <p class="mt-2 leading-relaxed">
        L’utilisateur de cette application consent à partager ses données.
        Celles-ci sont exclusivement destinées aux besoins du Service Contrôle.
        Tout abus entraînera la suspension, voire la fermeture du service.
      </p>
    </footer>

  </div>

  <script>
    const morningSlots = [
      "R1", "R3", "R5",
      "M1", "M3", "M5", "M7",
      "E1", "E3", "E5",
      "P1", "P3", "P5",
      "F1", "F3", "F5"
    ];

    const afternoonSlots = [
      "R2", "R4", "R6",
      "M2", "M4", "M6", "M8",
      "E2", "E4", "E6",
      "P2", "P4", "P6",
      "F2", "F4", "F6"
    ];

    const datePicker = document.getElementById("selectedDate");
    const reposBox = document.getElementById("repos");
    const changesBox = document.getElementById("changes");
    const badge = document.getElementById("badge");

    function renderSlots(containerId, slots) {
      document.getElementById(containerId).innerHTML =
        slots.map(s => `
          <div class="flex items-center gap-2">
            <span class="w-8 text-xs font-bold text-slate-500">${s}</span>
            <input
              type="text"
              data-slot="${s}"
              class="slot-input flex-1 border border-slate-200 rounded px-2 py-1 text-sm bg-slate-50 focus:bg-white focus:border-blue-500 outline-none"
              placeholder="Nom de l'agent"
            >
          </div>
        `).join("");
    }

    renderSlots("morningInputs", morningSlots);
    renderSlots("afternoonInputs", afternoonSlots);

    // Date locale, sans décalage lié au fuseau horaire
    function getLocalDate() {
      const now = new Date();
      const year = now.getFullYear();
      const month = String(now.getMonth() + 1).padStart(2, "0");
      const day = String(now.getDate()).padStart(2, "0");

      return `${year}-${month}-${day}`;
    }

    datePicker.value = getLocalDate();

    let loadingData = false;

    async function loadData() {
      loadingData = true;

      try {
        const res = await fetch(
          `/api/day/${encodeURIComponent(datePicker.value)}`
        );

        if (!res.ok) {
          throw new Error("Erreur lors du chargement");
        }

        const data = await res.json();

        document.querySelectorAll(".slot-input").forEach(input => {
          input.value = data.shifts[input.dataset.slot] || "";
        });

        reposBox.value = data.repos || "";
        changesBox.value = data.changes || "";

      } catch (error) {
        console.error(error);
        setBadge("Erreur de chargement", "rose");
      } finally {
        loadingData = false;
      }
    }

    function setBadge(text, color) {
      badge.textContent = text;
      badge.className =
        `px-2 py-1 text-xs rounded-full bg-${color}-100 text-${color}-700 font-semibold`;
    }

    // --- Envoi : un champ à la fois ---
    const timers = {};
    const pending = {};   // champs en cours de saisie / d'envoi

    function sendField(field, value) {
      const date = datePicker.value;   // date figée au moment de la saisie
      const token = {};
      pending[field] = token;   // jeton unique par saisie
      clearTimeout(timers[field]);

      timers[field] = setTimeout(async () => {
        try {
          const res = await fetch("/api/patch", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ date, field, value })
          });
          if (!res.ok) throw new Error("Erreur lors de la sauvegarde");
          setBadge("Connecté", "emerald");
        } catch (error) {
          console.error(error);
          setBadge("Erreur de sauvegarde", "rose");
        } finally {
          // Libère le champ seulement si aucune saisie plus récente n'a eu lieu
          if (pending[field] === token) delete pending[field];
        }
      }, 350);
    }

    document.addEventListener("input", event => {
      if (loadingData) return;
      const t = event.target;

      if (t.matches(".slot-input")) {
        sendField(t.dataset.slot, t.value.trim());
      } else if (t.id === "repos" || t.id === "changes") {
        sendField(t.id, t.value);
      }
    });

    datePicker.addEventListener("change", loadData);

    // --- Réception : on ne touche qu'au champ modifié ---
    function applyRemote(field, value) {
      if (pending[field]) return;   // ne pas écraser ce que je suis en train de taper

      const el =
        field === "repos" ? reposBox :
        field === "changes" ? changesBox :
        document.querySelector(`.slot-input[data-slot="${field}"]`);

      if (el && el.value !== value) el.value = value;
    }

    function connectWS() {
      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      const ws = new WebSocket(`${proto}//${location.host}/ws`);

      ws.onopen = () => {
        setBadge("Connecté", "emerald");
        loadData();   // rattrape ce qui a été manqué pendant une coupure
      };

      ws.onclose = () => {
        setBadge("Déconnecté", "amber");
        setTimeout(connectWS, 2000);   // reconnexion automatique
      };

      ws.onerror = () => ws.close();

      ws.onmessage = event => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.date === datePicker.value) {
            applyRemote(msg.field, msg.value);
          }
        } catch (error) {
          console.error("Message de synchronisation invalide", error);
        }
      };
    }

    connectWS();
  </script>
</body>
</html>
"""