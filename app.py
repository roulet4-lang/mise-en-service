import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI()

# URL de la base PostgreSQL fournie par Render
# Si DATABASE_URL n'est pas encore définie, il utilise SQLite temporairement
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_connection():
    if DATABASE_URL:
        # Corrige le préfixe si Render fournit postgres:// au lieu de postgresql://
        url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(url)
        return conn
    else:
        import sqlite3
        conn = sqlite3.connect("services.db", check_same_thread=False)
        return conn

def init_db():
    conn = get_connection()
    cur = conn.cursor()
    if DATABASE_URL:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_services (
                date_str VARCHAR(20) PRIMARY KEY,
                shifts_json TEXT,
                repos_text TEXT,
                changes_text TEXT
            );
        """)
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_services (
                date_str TEXT PRIMARY KEY,
                shifts_json TEXT,
                repos_text TEXT,
                changes_text TEXT
            );
        """)
    conn.commit()
    cur.close()
    conn.close()

init_db()

class ShiftData(BaseModel):
    date: str
    shifts: Dict[str, str]
    repos: str
    changes: str

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

@app.get("/api/day/{date_str}")
def get_day(date_str: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT shifts_json, repos_text, changes_text FROM daily_services WHERE date_str = %s" if DATABASE_URL else "SELECT shifts_json, repos_text, changes_text FROM daily_services WHERE date_str = ?", (date_str,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return {
            "shifts": json.loads(row[0]),
            "repos": row[1] or "",
            "changes": row[2] or ""
        }
    return {"shifts": {}, "repos": "", "changes": ""}

@app.post("/api/save")
async def save_day(payload: ShiftData):
    conn = get_connection()
    cur = conn.cursor()
    if DATABASE_URL:
        cur.execute("""
            INSERT INTO daily_services (date_str, shifts_json, repos_text, changes_text)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date_str) DO UPDATE SET
                shifts_json = EXCLUDED.shifts_json,
                repos_text = EXCLUDED.repos_text,
                changes_text = EXCLUDED.changes_text;
        """, (payload.date, json.dumps(payload.shifts), payload.repos, payload.changes))
    else:
        cur.execute("""
            INSERT INTO daily_services (date_str, shifts_json, repos_text, changes_text)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(date_str) DO UPDATE SET
                shifts_json = excluded.shifts_json,
                repos_text = excluded.repos_text,
                changes_text = excluded.changes_text;
        """, (payload.date, json.dumps(payload.shifts), payload.repos, payload.changes))
    conn.commit()
    cur.close()
    conn.close()

    await manager.broadcast({
        "date": payload.date,
        "shifts": payload.shifts,
        "repos": payload.repos,
        "changes": payload.changes
    })
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
    <!-- Header -->
    <div class="bg-white p-4 rounded-xl shadow-sm border border-slate-200 flex flex-wrap justify-between items-center gap-3">
      <div>
        <h1 class="text-lg font-bold text-slate-900">Service Contrôle</h1>
        <p class="text-xs text-slate-500">Mise à jour en temps réel & Sauvegarde permanente</p>
      </div>
      <div class="flex items-center gap-3">
        <input type="date" id="selectedDate" class="border rounded-lg px-3 py-1.5 font-medium bg-slate-50 border-slate-300 text-sm">
        <span id="badge" class="px-2 py-1 text-xs rounded-full bg-emerald-100 text-emerald-700 font-semibold">En direct</span>
      </div>
    </div>

    <!-- Grille des shifts -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">
        <h2 class="font-bold text-amber-700 text-sm border-b pb-2 mb-3">Postes Matin</h2>
        <div id="morningInputs" class="space-y-2"></div>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">
        <h2 class="font-bold text-indigo-700 text-sm border-b pb-2 mb-3">Postes Après-midi</h2>
        <div id="afternoonInputs" class="space-y-2"></div>
      </div>
    </div>

    <!-- Repos & Changements -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">
        <h2 class="font-bold text-slate-700 text-sm border-b pb-2 mb-2">Agents en Repos</h2>
        <textarea id="repos" rows="4" placeholder="Un nom par ligne..." class="w-full border border-slate-300 rounded-lg p-2 text-sm outline-none focus:border-blue-500"></textarea>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">
        <h2 class="font-bold text-rose-700 text-sm border-b pb-2 mb-2">Changements de service</h2>
        <textarea id="changes" rows="4" placeholder="Ex: COSENTINO E4 -> MAELLE P6" class="w-full border border-slate-300 rounded-lg p-2 text-sm outline-none focus:border-blue-500"></textarea>
      </div>
    </div>
  </div>

  <script>
    const morningSlots = ["R1", "R3", "R5", "M1", "M3", "M5", "M7", "E1", "E3", "E5", "P1", "P3", "P5", "F1", "F3", "F5"];
    const afternoonSlots = ["R2", "R4", "R6", "M2", "M4", "M6", "M8", "E2", "E4", "E6", "P2", "P4", "P6", "F2", "F4", "F6"];
    const datePicker = document.getElementById("selectedDate");
    const reposBox = document.getElementById("repos");
    const changesBox = document.getElementById("changes");

    function renderSlots(containerId, slots) {
      document.getElementById(containerId).innerHTML = slots.map(s => `
        <div class="flex items-center gap-2">
          <span class="w-8 text-xs font-bold text-slate-500">${s}</span>
          <input type="text" data-slot="${s}" class="slot-input flex-1 border border-slate-200 rounded px-2 py-1 text-sm bg-slate-50 focus:bg-white focus:border-blue-500 outline-none" placeholder="Nom de l'agent">
        </div>
      `).join('');
    }
    renderSlots("morningInputs", morningSlots);
    renderSlots("afternoonInputs", afternoonSlots);

    datePicker.value = new Date().toISOString().split('T')[0];

    async function loadData() {
      const res = await fetch(`/api/day/${datePicker.value}`);
      const data = await res.json();
      document.querySelectorAll(".slot-input").forEach(i => i.value = data.shifts[i.dataset.slot] || "");
      reposBox.value = data.repos || "";
      changesBox.value = data.changes || "";
    }

    let saveTimer;
    function triggerAutoSave() {
      clearTimeout(saveTimer);
      saveTimer = setTimeout(async () => {
        const shifts = {};
        document.querySelectorAll(".slot-input").forEach(i => {
          if (i.value.trim()) shifts[i.dataset.slot] = i.value.trim();
        });
        await fetch("/api/save", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            date: datePicker.value,
            shifts: shifts,
            repos: reposBox.value,
            changes: changesBox.value
          })
        });
      }, 350);
    }

    document.addEventListener("input", e => {
      if (e.target.matches(".slot-input, #repos, #changes")) triggerAutoSave();
    });

    datePicker.addEventListener("change", loadData);

    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${location.host}/ws`);
    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.date === datePicker.value) {
        document.querySelectorAll(".slot-input").forEach(i => i.value = msg.shifts[i.dataset.slot] || "");
        reposBox.value = msg.repos || "";
        changesBox.value = msg.changes || "";
      }
    };

    loadData();
  </script>
</body>
</html>
"""