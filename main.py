from fastapi import FastAPI, HTTPException
import sqlite3, os

DB_PATH = os.getenv("DB_PATH", "data.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) if "/" in DB_PATH else None

def conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
    return c

app = FastAPI()

@app.get("/health")
def health(): return {"ok": True}

@app.get("/set")
def set_(key: str, val: str):
    c = conn()
    c.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, val))
    c.commit(); c.close()
    return {"set": {key: val}}

@app.get("/get")
def get_(key: str):
    c = conn()
    cur = c.execute("SELECT v FROM kv WHERE k=?", (key,)); row = cur.fetchone(); c.close()
    if not row: raise HTTPException(status_code=404, detail="key not found")
    return {"key": key, "val": row[0]}

@app.get("/keys")
def keys():
    c = conn()
    rows = [r[0] for r in c.execute("SELECT k FROM kv ORDER BY k")]
    c.close()
    return {"keys": rows}
