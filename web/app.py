import asyncio
import io
import os
import shutil
import subprocess
import urllib.parse
import contextlib
from datetime import datetime
from pathlib import Path

import asyncpg
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from settings import get_settings, get_db_backup_config
from db_utils import fetch_databases
from backup import do_backup
from restore import list_dump_files, restore_dump, run_psql

DB_HOST, DB_SUPERUSER, DB_SUPERUSER_PASSWORD, YAML_CFG = get_settings()

app = FastAPI(title="laPG")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


async def get_db_sizes():
    for db in ("postgres", "template1"):
        try:
            conn = await asyncpg.connect(
                host=DB_HOST, user=DB_SUPERUSER,
                password=DB_SUPERUSER_PASSWORD, database=db, timeout=5,
            )
            rows = await conn.fetch("""
                SELECT datname,
                       pg_database_size(datname) AS size_bytes
                FROM pg_database
                WHERE datistemplate = false
                ORDER BY datname
            """)
            await conn.close()
            return [(r["datname"], r["size_bytes"]) for r in rows]
        except (asyncpg.PostgresError, asyncio.TimeoutError, ConnectionError):
            continue
    return []


def get_backup_info():
    entries = list_dump_files()
    result = []
    for e in entries:
        size = os.path.getsize(e)
        mtime = datetime.fromtimestamp(os.path.getmtime(e))
        result.append({"path": e, "size": size, "mtime": mtime})
    return result[-20:]


def get_disk_usage():
    usage = shutil.disk_usage(".")
    return {
        "total": usage.total,
        "used": usage.used,
        "free": usage.free,
        "percent": usage.used / usage.total * 100,
    }


async def _check_connection():
    try:
        await fetch_databases(
            host=DB_HOST, user=DB_SUPERUSER,
            password=DB_SUPERUSER_PASSWORD,
        )
        return True
    except Exception:
        return False


def _capture_output(func, *args, **kwargs):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ok = func(*args, **kwargs)
    return ok, buf.getvalue()


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    try:
        db_sizes = await get_db_sizes()
    except Exception:
        db_sizes = []
    conn_ok = len(db_sizes) > 0 or await _check_connection()
    dbs = [{"name": n, "size": s} for n, s in db_sizes]
    backups = get_backup_info()
    disk = get_disk_usage()
    return templates.TemplateResponse(request, "dashboard.html", {
        "connected": conn_ok,
        "databases": dbs,
        "backups": backups,
        "disk": disk,
    })


@app.get("/api/databases", response_class=HTMLResponse)
async def api_databases():
    conn_ok = await _check_connection()
    if not conn_ok:
        return '<div class="error">Нет подключения к PostgreSQL</div>'
    db_sizes = await get_db_sizes()
    lines = "".join(
        f'<tr><td>{n}</td><td>{_fmt_size(s)}</td>'
        f'<td><button class="btn btn-sm" hx-post="/backup/{n}" '
        f'hx-target="#result" hx-swap="innerHTML">Бэкап</button></td></tr>'
        for n, s in db_sizes
    )
    return f'<table class="table"><tr><th>БД</th><th>Размер</th><th></th></tr>{lines}</table>'


@app.get("/api/backups", response_class=HTMLResponse)
async def api_backups():
    backups = get_backup_info()
    if not backups:
        return '<div class="muted">Нет бэкапов</div>'
    lines = "".join(
        f'<tr><td>{os.path.basename(b["path"])}</td>'
        f'<td>{_fmt_size(b["size"])}</td>'
        f'<td>{b["mtime"].strftime("%Y-%m-%d %H:%M")}</td></tr>'
        for b in reversed(backups)
    )
    return f'<table class="table"><tr><th>Файл</th><th>Размер</th><th>Дата</th></tr>{lines}</table>'


@app.post("/backup/{db_name}", response_class=HTMLResponse)
async def backup_single(db_name: str):
    cfg = get_db_backup_config(db_name, YAML_CFG)
    loop = asyncio.get_event_loop()
    ok, output = await loop.run_in_executor(
        None, _capture_output, do_backup, db_name, cfg.compress, cfg.keep
    )
    status = "ok" if ok else "fail"
    return f'<div class="toast toast-{status}"><pre>{output}</pre></div>'


@app.post("/backup-all", response_class=HTMLResponse)
async def backup_all_route():
    from backup import backup_all as _backup_all

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ok = await _backup_all()
    output = buf.getvalue()
    status = "ok" if ok else "fail"
    return f'<div class="toast toast-{status}"><pre>{output}</pre></div>'


@app.get("/restore", response_class=HTMLResponse)
async def restore_page(request: Request):
    databases = []
    try:
        databases = await fetch_databases(
            host=DB_HOST, user=DB_SUPERUSER,
            password=DB_SUPERUSER_PASSWORD,
        )
    except Exception:
        pass
    backups = list_dump_files()
    return templates.TemplateResponse(request, "restore.html", {
        "databases": databases,
        "backups": [os.path.basename(b) for b in backups],
        "backup_paths": backups,
    })


@app.post("/restore", response_class=HTMLResponse)
async def restore_execute(
    target_db: str = Form(...),
    dump_file: str = Form(...),
    action: str = Form("restore"),
):
    backups = list_dump_files()
    full_path = None
    for b in backups:
        if os.path.basename(b) == dump_file:
            full_path = b
            break
    if not full_path:
        return '<div class="toast toast-fail">Файл не найден</div>'

    if action == "preview":
        from restore import extract_db_name_from_dump
        name = extract_db_name_from_dump(full_path)
        return f"<pre>БД в дампе: {name or 'не определена'}</pre>"

    buf = io.StringIO()
    env = os.environ.copy()
    if DB_SUPERUSER_PASSWORD:
        env["PGPASSWORD"] = DB_SUPERUSER_PASSWORD
    with contextlib.redirect_stdout(buf):
        check = f"SELECT 1 FROM pg_database WHERE datname = '{target_db}'"
        try:
            result = subprocess_run_psql("psql", check)
            exists = "1" in result.stdout
        except Exception as e:
            print(f"Ошибка проверки: {e}")
            exists = False

        if exists:
            print(f"Удаление {target_db}...")
            try:
                subprocess_run_psql("dropdb", target_db)
            except Exception as e:
                print(f"Ошибка удаления: {e}")
                output = buf.getvalue()
                return f'<div class="toast toast-fail"><pre>{output}</pre></div>'

        print(f"Создание {target_db}...")
        try:
            subprocess_run_psql("createdb", target_db)
        except Exception as e:
            print(f"Ошибка создания: {e}")
            output = buf.getvalue()
            return f'<div class="toast toast-fail"><pre>{output}</pre></div>'

        restore_dump(full_path, target_db)

    output = buf.getvalue()
    return f'<div class="toast toast-ok"><pre>{output}</pre></div>'


def subprocess_run_psql(action, target_db=""):
    cmd = [action, "-h", DB_HOST, "-U", DB_SUPERUSER]
    env = os.environ.copy()
    if DB_SUPERUSER_PASSWORD:
        env["PGPASSWORD"] = DB_SUPERUSER_PASSWORD
    if action in ("dropdb", "createdb"):
        cmd.append(target_db)
    else:
        cmd.extend(["-d", "postgres", "-t", "-c", target_db])
    import subprocess
    return subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)


_ssh_sessions: dict[str, str] = {}


def _has_sshpass():
    return shutil.which("sshpass") is not None


def _ssh_key_ok(host, port, user):
    try:
        r = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
             "-o", "StrictHostKeyChecking=accept-new",
             "-p", str(port), f"{user}@{host}", "echo OK"],
            capture_output=True, text=True, timeout=10,
        )
        return r.returncode == 0 and r.stdout.strip() == "OK"
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _ssh_pass_ok(host, port, user, password):
    try:
        r = subprocess.run(
            ["sshpass", "-p", password, "ssh",
             "-o", "ConnectTimeout=5",
             "-o", "StrictHostKeyChecking=accept-new",
             "-p", str(port), f"{user}@{host}", "echo OK"],
            capture_output=True, text=True, timeout=10,
        )
        return r.returncode == 0 and r.stdout.strip() == "OK"
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


@app.get("/servers", response_class=HTMLResponse)
async def servers_page(request: Request):
    return templates.TemplateResponse(request, "servers.html", {
        "servers": YAML_CFG.servers,
    })


@app.get("/servers/{name}/status", response_class=HTMLResponse)
async def server_status(name: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'

    if _ssh_key_ok(server.host, server.port, server.user):
        return _server_status_html(name, "connected", f"{server.user}@{server.host} — ключ")
    if name in _ssh_sessions:
        if _ssh_pass_ok(server.host, server.port, server.user, _ssh_sessions[name]):
            return _server_status_html(name, "connected", f"{server.user}@{server.host} — пароль")

    if _has_sshpass():
        form = (
            f'<form hx-post="/servers/{name}/connect" hx-target="#server-{name}">'
            f'<input type="password" name="password" placeholder="Пароль" required>'
            f'<button class="btn btn-sm">Подключиться</button>'
            f'</form>'
        )
    else:
        form = '<span class="muted">Установите sshpass для входа по паролю</span>'
    return _server_status_html(name, "password", form)


def _server_status_html(name, status, body):
    colors = {"connected": "green", "password": "orange", "error": "red"}
    labels = {"connected": "Подключено", "password": "Требуется пароль", "error": "Ошибка"}
    color = colors.get(status, "red")
    label = labels.get(status, status)
    return f'<div id="server-{name}"><span class="badge badge-{color}">{label}</span> {body}</div>'


@app.post("/servers/{name}/connect", response_class=HTMLResponse)
async def server_connect(name: str, password: str = Form(...)):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'

    if _ssh_pass_ok(server.host, server.port, server.user, password):
        _ssh_sessions[name] = password
        return _server_status_html(name, "connected", f"{server.user}@{server.host} — пароль")

    return _server_status_html(name, "error",
        '<span class="muted">Неверный пароль</span> '
        f'<button class="btn btn-sm" hx-get="/servers/{name}/status" '
        f'hx-target="#server-{name}">Повторить</button>'
    )


def _ssh_run_cmd(server, cmd):
    """Run a command on remote server. Returns (stdout, stderr, rc) or None."""
    base = [
        "ssh", "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=5",
        "-o", "StrictHostKeyChecking=accept-new",
        "-p", str(server.port), f"{server.user}@{server.host}",
    ]
    try:
        r = subprocess.run(base + [cmd], capture_output=True, text=True, timeout=15)
        if r.returncode != 255:
            return r.stdout, r.stderr, r.returncode
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None

    if server.name in _ssh_sessions and _has_sshpass():
        try:
            pw = _ssh_sessions[server.name]
            r = subprocess.run(
                ["sshpass", "-p", pw] + base + [cmd],
                capture_output=True, text=True, timeout=15,
            )
            return r.stdout, r.stderr, r.returncode
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
    return None


@app.get("/servers/{name}/sqlite", response_class=HTMLResponse)
async def server_sqlite(name: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'

    result = _ssh_run_cmd(server,
        r"find /home /root /tmp /opt \( -name '*.db' -o -name '*.sqlite' -o -name '*.sqlite3' \) -printf '%s\t%T@\t%p\n' 2>/dev/null"
    )
    if result is None:
        return '<div class="toast toast-fail">Ошибка подключения</div>'

    stdout, _, rc = result
    if rc != 0 and not stdout.strip():
        return '<div class="muted">Нет доступа или .db не найдены</div>'

    files = []
    for line in stdout.strip().splitlines():
        parts = line.split("\t", 2)
        if len(parts) == 3:
            size, epoch, path = parts
            from datetime import datetime as dt
            mtime = dt.fromtimestamp(float(epoch)).strftime("%Y-%m-%d %H:%M")
            access = "редактирование" if path.startswith("/home") else "нет доступа"
            files.append({"path": path, "size": _fmt_size(int(size)), "mtime": mtime, "access": access})

    if not files:
        return '<div class="muted">Базы SQLite не найдены</div>'

    rows = "".join(
        f"<tr><td>{f['path']}</td><td>{f['size']}</td><td>{f['mtime']}</td>"
        f"<td><span class=\"badge badge-{'green' if f['access'] == 'редактирование' else 'red'}\">{f['access']}</span></td>"
        + (
            f'<td><a class="btn btn-sm btn-edit" href="/sqlite/{urllib.parse.quote(name)}/browse?path={urllib.parse.quote(f["path"])}" title="редактировать данные">\U0001F589</a></td>'
            if f['access'] == 'редактирование' else '<td></td>'
        )
        + "</tr>"
        for f in files
    )
    return f'<table class="table"><tr><th>Путь</th><th>Размер</th><th>Изменён</th><th>Доступ</th><th></th></tr>{rows}</table>'


@app.get("/sqlite/{name}/browse", response_class=HTMLResponse)
async def sqlite_browse(request: Request, name: str, path: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return templates.TemplateResponse(request, "sqlite_browse.html", {
            "error": "Сервер не найден", "db_path": path, "tables": [],
        })

    cmd = (
        "python3 << 'PYEOF'\n"
        "import sqlite3\n"
        f'conn = sqlite3.connect("{path}")\n'
        "for t in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\"):\n"
        "  print(t[0])\n"
        "conn.close()\n"
        "PYEOF"
    )
    result = _ssh_run_cmd(server, cmd)
    tables = []
    if result is not None:
        stdout, stderr, rc = result
        if rc != 0:
            print(f"sqlite browse error: {stderr}")
        elif stdout.strip():
            tables = [l.strip() for l in stdout.strip().splitlines() if l.strip()]

    return templates.TemplateResponse(request, "sqlite_browse.html", {
        "server_name": name, "db_path": path, "db_path_enc": urllib.parse.quote(path),
        "tables": tables, "error": None,
    })


def _get_column_types(server, path, table):
    cmd = (
        "python3 << 'PYEOF'\n"
        "import sqlite3, json\n"
        f'conn = sqlite3.connect("{path}")\n'
        f'c = conn.execute("PRAGMA table_info(\\"{table}\\")")\n'
        "info = [{'name': r[1], 'type': r[2]} for r in c.fetchall()]\n"
        "conn.close()\n"
        "print(json.dumps(info))\n"
        "PYEOF"
    )
    result = _ssh_run_cmd(server, cmd)
    if result is None:
        return {}
    try:
        import json
        info = json.loads(result[0])
        return {r["name"]: r["type"].upper() for r in info}
    except (json.JSONDecodeError, KeyError):
        return {}


def _detect_bool_cols(col_types, display_cols, rows):
    bool_cols = set()
    for c in display_cols:
        t = col_types.get(c, "")
        if "INT" in t or "BOOL" in t:
            vals = {r.get(c) for r in rows if r.get(c) is not None}
            if vals.issubset({0, 1}):
                bool_cols.add(c)
    return bool_cols


@app.get("/sqlite/{name}/table", response_class=HTMLResponse)
async def sqlite_table(request: Request, name: str, path: str, table: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'

    col_types_json = _get_column_types(server, path, table)

    cmd = (
        "python3 << 'PYEOF'\n"
        "import sqlite3, json\n"
        f'conn = sqlite3.connect("{path}")\n'
        "conn.row_factory = sqlite3.Row\n"
        f'c = conn.execute("SELECT *, rowid AS _rid FROM \\"{table}\\" LIMIT 200")\n'
        "rows = [dict(r) for r in c.fetchall()]\n"
        "cols = [d[0] for d in c.description]\n"
        "print(json.dumps({\"columns\": cols, \"rows\": rows}))\n"
        "conn.close()\n"
        "PYEOF"
    )
    result = _ssh_run_cmd(server, cmd)
    columns, rows = [], []
    error = None
    if result is not None:
        stdout, stderr, rc = result
        if rc == 0 and stdout.strip():
            try:
                import json
                data = json.loads(stdout.strip())
                columns = data.get("columns", [])
                rows = data.get("rows", [])
            except json.JSONDecodeError:
                error = f"Ошибка парсинга: {stderr}"
        else:
            error = stderr or "Нет данных"
    else:
        error = "Ошибка подключения"

    display_cols = [c for c in columns if c not in ("_rid", "rowid")]
    bool_cols = _detect_bool_cols(col_types_json, display_cols, rows)

    return templates.TemplateResponse(request, "sqlite_table.html", {
        "server_name": name, "db_path": path, "db_path_enc": urllib.parse.quote(path),
        "table": table, "columns": display_cols, "rows": rows,
        "bool_cols": bool_cols, "error": error,
    })


@app.get("/sqlite/{name}/table/edit-row", response_class=HTMLResponse)
async def sqlite_edit_row(name: str, path: str, table: str, rowid: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'

    cmd = (
        "python3 << 'PYEOF'\n"
        "import sqlite3, json\n"
        f'conn = sqlite3.connect("{path}")\n'
        "conn.row_factory = sqlite3.Row\n"
        f'c = conn.execute("SELECT * FROM \\"{table}\\" WHERE rowid = {rowid}")\n'
        "row = [dict(r) for r in c.fetchall()]\n"
        "cols = [d[0] for d in c.description]\n"
        "print(json.dumps({\"columns\": cols, \"rows\": row}))\n"
        "conn.close()\n"
        "PYEOF"
    )
    result = _ssh_run_cmd(server, cmd)
    if result is None:
        return '<div class="toast toast-fail">Ошибка подключения</div>'
    stdout, _, rc = result
    if rc != 0 or not stdout.strip():
        return '<div class="toast toast-fail">Строка не найдена</div>'

    import json as _json
    data = _json.loads(stdout.strip())
    cols = data.get("columns", [])
    rows = data.get("rows", [])
    if not rows:
        return '<div class="toast toast-fail">Строка не найдена</div>'

    row = rows[0]
    actual_rowid = row.get("rowid", rowid)
    data_cols = [c for c in cols if c not in ("rowid",)]

    col_types = _get_column_types(server, path, table)
    bool_cols = set()
    for c in data_cols:
        t = col_types.get(c, "").upper()
        v = row.get(c)
        if ("INT" in t or "BOOL" in t) and v in (0, 1):
            bool_cols.add(c)

    def _cell(i, c):
        if c in bool_cols:
            checked = 'checked' if row.get(c) == 1 else ''
            return (
                f'<td><input type="hidden" name="v{i}" value="0">'
                f'<input type="checkbox" name="v{i}" value="1" {checked} class="edit-cb"></td>'
            )
        val = "NULL" if row.get(c) is None else str(row.get(c))
        return f'<td><input name="v{i}" value="{val}" class="edit-input"></td>'

    inputs = "".join(_cell(i, c) for i, c in enumerate(data_cols))
    cols_json = _json.dumps(list(data_cols))
    return (
        f'<tr id="row-edit-{actual_rowid}">'
        f'{inputs}'
        f'<td>'
        f'<form hx-post="/sqlite/{name}/table/update" hx-target="#row-edit-{actual_rowid}" hx-swap="outerHTML">'
        f'<input type="hidden" name="path" value="{path}">'
        f'<input type="hidden" name="table" value="{table}">'
        f'<input type="hidden" name="rowid" value="{actual_rowid}">'
        f'<input type="hidden" name="cols" value=\'{cols_json}\'>'
        f'<button class="btn btn-sm btn-primary">Save</button>'
        f'</form>'
        f'</td>'
        f'</tr>'
    )


@app.post("/sqlite/{name}/table/update", response_class=HTMLResponse)
async def sqlite_table_update(request: Request, name: str):
    form = await request.form()
    path = form.get("path", "")
    table = form.get("table", "")
    rowid = form.get("rowid", "")
    cols_raw = form.get("cols", "[]")

    import json as _json
    try:
        data_cols = _json.loads(cols_raw)
    except _json.JSONDecodeError:
        return '<div class="toast toast-fail">Ошибка данных</div>'

    # Take last value per key (checkbox sends hidden 0 then checkbox 1)
    vals = {}
    for k, v in form.multi_items():
        vals[k] = v

    sets = []
    for i, colname in enumerate(data_cols):
        val = vals.get(f"v{i}", "")
        escaped = val.replace("'", "''")
        sets.append(f'"{colname}" = \'{escaped}\'')

    set_clause = ", ".join(sets)
    cmd = (
        "python3 << 'PYEOF'\n"
        "import sqlite3\n"
        f'conn = sqlite3.connect("{path}")\n'
        f'conn.execute("UPDATE \\"{table}\\" SET {set_clause} WHERE rowid = {rowid}")\n'
        "conn.commit()\n"
        "conn.close()\n"
        "print('OK')\n"
        "PYEOF"
    )

    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if server:
        result = _ssh_run_cmd(server, cmd)
        if result is None:
            return '<div class="toast toast-fail">Ошибка подключения</div>'
        stdout, _, rc = result
        if rc != 0 or stdout.strip() != "OK":
            return f'<div class="toast toast-fail">Ошибка: {stdout[:500]}</div>'

    return await sqlite_edit_row(name, path, table, rowid)


def _fmt_size(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
