import sqlite3
import json
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "db.db"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS _services (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            server_name     TEXT,
            project_name    TEXT NOT NULL UNIQUE,
            repo_url        TEXT,
            base_path       TEXT NOT NULL DEFAULT '/home/project/bots/',
            systemd_user    TEXT,
            port            INTEGER,
            entry_point     TEXT,
            git_branch      TEXT DEFAULT 'main',
            status          TEXT DEFAULT 'pending',
            description     TEXT,
            extra_env       TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TRIGGER IF NOT EXISTS trg_services_updated
            AFTER UPDATE ON _services
            FOR EACH ROW
        BEGIN
            UPDATE _services SET updated_at = datetime('now') WHERE id = OLD.id;
        END;
    """)
    # Add server_name column if table already existed without it
    cols = {row[1] for row in conn.execute("PRAGMA table_info(_services)").fetchall()}
    if "server_name" not in cols:
        conn.execute("ALTER TABLE _services ADD COLUMN server_name TEXT")
    if "db_type" not in cols:
        conn.execute("ALTER TABLE _services ADD COLUMN db_type TEXT DEFAULT 'postgresql'")
    if "db_name" not in cols:
        conn.execute("ALTER TABLE _services ADD COLUMN db_name TEXT")
    conn.commit()
    conn.close()


def migrate_favorites(favorites: dict[str, list[str]]):
    """Migrate bot favorites from settings.yaml into _services table."""
    conn = _get_conn()
    existing = {r["project_name"] for r in conn.execute("SELECT project_name FROM _services").fetchall()}
    added = 0
    for server_name, services in favorites.items():
        for svc in services:
            if svc not in existing:
                conn.execute(
                    "INSERT INTO _services (server_name, project_name, status) VALUES (?, ?, 'deployed')",
                    (server_name, svc),
                )
                added += 1
                existing.add(svc)
    if added:
        conn.commit()
    conn.close()
    return added


def list_services() -> list[dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM _services ORDER BY project_name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_service(service_id: int) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM _services WHERE id = ?", (service_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_service_by_name(name: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM _services WHERE project_name = ?", (name,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_service(**kw) -> int:
    conn = _get_conn()
    cols = []
    vals = []
    for k, v in kw.items():
        if k in ("extra_env",) and isinstance(v, dict):
            v = json.dumps(v)
        cols.append(k)
        vals.append(v)
    placeholders = ",".join("?" for _ in cols)
    sql = f"INSERT INTO _services ({','.join(cols)}) VALUES ({placeholders})"
    cur = conn.execute(sql, vals)
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id


def update_service(service_id: int, **kw) -> bool:
    conn = _get_conn()
    sets = []
    vals = []
    for k, v in kw.items():
        if k in ("extra_env",) and isinstance(v, dict):
            v = json.dumps(v)
        sets.append(f"{k} = ?")
        vals.append(v)
    if not sets:
        conn.close()
        return False
    vals.append(service_id)
    sql = f"UPDATE _services SET {','.join(sets)} WHERE id = ?"
    cur = conn.execute(sql, vals)
    conn.commit()
    ok = cur.rowcount > 0
    conn.close()
    return ok


def delete_service(service_id: int) -> bool:
    conn = _get_conn()
    cur = conn.execute("DELETE FROM _services WHERE id = ?", (service_id,))
    conn.commit()
    ok = cur.rowcount > 0
    conn.close()
    return ok
