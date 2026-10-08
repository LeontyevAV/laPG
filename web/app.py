import asyncio
import base64
import io
import json
import os
import re
import shutil
import shlex
import subprocess
import urllib.parse
import contextlib
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent

import asyncpg
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from settings import get_settings, get_db_backup_config
from db import init_db as init_services_db, migrate_favorites, create_service, update_service, list_services, get_service, audit_log, list_audit_logs, count_audit_logs


@asynccontextmanager
async def _lifespan(app: FastAPI):
    init_services_db()
    fav = getattr(YAML_CFG.bots, "favorites", {}) if hasattr(YAML_CFG, "bots") else {}
    if fav:
        added = migrate_favorites(fav)
        if added:
            print(f"[db] Migrated {added} favorites to _services")
    yield
from db_utils import fetch_databases
from backup import do_backup
from restore import list_dump_files, restore_dump, run_psql
from compare import _fetch_schema, _normalize_schema, build_diffs
from cron import get_crontab, set_crontab, parse_lines, list_project_entries, restart_cron, build_cron_line, build_backup_command

DB_HOST, DB_SUPERUSER, DB_SUPERUSER_PASSWORD, YAML_CFG = get_settings()

app = FastAPI(title="laPG", lifespan=_lifespan)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")

TOOLS = [
    {"id": "python3.10", "name": "Python 3.10", "icon": "🐍", "category": "python",
     "check": "command -v python3.10 2>/dev/null && python3.10 --version 2>&1 || true",
     "install": "sudo add-apt-repository -y ppa:deadsnakes/ppa >/dev/null 2>&1; sudo apt update -qq 2>/dev/null; sudo apt install -y python3.10 python3.10-venv python3.10-dev 2>&1 | tail -5",
     "desc": "Python 3.10 (через deadsnakes)"},
    {"id": "python3.11", "name": "Python 3.11", "icon": "🐍", "category": "python",
     "check": "command -v python3.11 2>/dev/null && python3.11 --version 2>&1 || true",
     "install": "sudo add-apt-repository -y ppa:deadsnakes/ppa >/dev/null 2>&1; sudo apt update -qq 2>/dev/null; sudo apt install -y python3.11 python3.11-venv python3.11-dev 2>&1 | tail -5",
     "desc": "Python 3.11 (через deadsnakes)"},
    {"id": "python3.12", "name": "Python 3.12", "icon": "🐍", "category": "python",
     "check": "command -v python3.12 2>/dev/null && python3.12 --version 2>&1 || true",
     "install": "sudo add-apt-repository -y ppa:deadsnakes/ppa >/dev/null 2>&1; sudo apt update -qq 2>/dev/null; sudo apt install -y python3.12 python3.12-venv python3.12-dev 2>&1 | tail -5",
     "desc": "Python 3.12 (через deadsnakes)"},
    {"id": "python3.13", "name": "Python 3.13", "icon": "🐍", "category": "python",
     "check": "command -v python3.13 2>/dev/null && python3.13 --version 2>&1 || true",
     "install": "sudo add-apt-repository -y ppa:deadsnakes/ppa >/dev/null 2>&1; sudo apt update -qq 2>/dev/null; sudo apt install -y python3.13 python3.13-venv python3.13-dev 2>&1 | tail -5",
     "desc": "Python 3.13 (через deadsnakes)"},
    {"id": "python3.14", "name": "Python 3.14", "icon": "🐍", "category": "python",
     "check": "command -v python3.14 2>/dev/null && python3.14 --version 2>&1 || true",
     "install": "sudo add-apt-repository -y ppa:deadsnakes/ppa >/dev/null 2>&1; sudo apt update -qq 2>/dev/null; sudo apt install -y python3.14 python3.14-venv python3.14-dev 2>&1 | tail -5",
     "desc": "Python 3.14 (через deadsnakes)"},
    {"id": "nginx", "name": "Nginx", "icon": "🌐", "category": "web",
     "check": "nginx -v 2>&1 || true",
     "install": "sudo apt update -qq 2>/dev/null; sudo apt install -y nginx 2>&1 | tail -5",
     "desc": "Веб-сервер Nginx"},
    {"id": "nodejs", "name": "Node.js", "icon": "📦", "category": "runtime",
     "check": "node --version 2>&1 || true",
     "install": "curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash - >/dev/null 2>&1; sudo apt install -y nodejs 2>&1 | tail -5",
     "desc": "Node.js 20 LTS"},
    {"id": "git", "name": "Git", "icon": "🔀", "category": "dev",
     "check": "git --version 2>&1 || true",
     "install": "sudo apt update -qq 2>/dev/null; sudo apt install -y git 2>&1 | tail -3",
     "desc": "Система контроля версий"},
    {"id": "htop", "name": "htop", "icon": "📊", "category": "utils",
     "check": "command -v htop 2>/dev/null && htop --version 2>&1 || true",
     "install": "sudo apt update -qq 2>/dev/null; sudo apt install -y htop 2>&1 | tail -3",
     "desc": "Мониторинг процессов"},
    {"id": "redis", "name": "Redis", "icon": "🟥", "category": "data",
     "check": "redis-cli --version 2>&1 || true",
     "install": "sudo apt update -qq 2>/dev/null; sudo apt install -y redis-server 2>&1 | tail -5",
     "desc": "In-memory data store"},
]


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


@app.get("/.well-known/{path:path}", include_in_schema=False)
async def well_known_noop():
    return Response(status_code=204)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon_ico():
    return Response(status_code=204)


@app.get("/favicon.svg", include_in_schema=False)
async def favicon_svg():
    svg = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">
  <rect width="32" height="32" rx="6" fill="#2563eb"/>
  <text x="16" y="22" text-anchor="middle" font-size="15" font-weight="bold" fill="white" font-family="sans-serif">AP</text>
</svg>'''
    return Response(content=svg.encode(), media_type="image/svg+xml")


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
        "servers": YAML_CFG.servers,
    })


CATEGORY_LABELS = {"python": "Python", "web": "Веб-серверы", "runtime": "Среды выполнения", "dev": "Разработка", "utils": "Утилиты", "data": "Базы данных"}

@app.get("/tools", response_class=HTMLResponse)
async def tools_page(request: Request):
    return templates.TemplateResponse(request, "tools.html", {
        "servers": YAML_CFG.servers,
        "tools": TOOLS,
        "categories": CATEGORY_LABELS,
    })


DEPLOY_SH = PROJECT_DIR / "deploy.sh"


@app.post("/api/self-update")
async def self_update():
    import html as _html
    import tempfile
    if not DEPLOY_SH.exists():
        return '<div class="toast toast-fail">deploy.sh не найден</div>'
    try:
        log_path = PROJECT_DIR / "tmp" / "self-update.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "w") as f:
            f.write(f"=== laPG deploy {datetime.now().isoformat()} ===\n")
        with open(log_path, "a") as f:
            proc = subprocess.Popen(
                ["bash", str(DEPLOY_SH)],
                stdout=f, stderr=subprocess.STDOUT,
                cwd=str(PROJECT_DIR),
                start_new_session=True,
            )
        audit_log(action="self-update", status="started", details="deploy.sh запущен")
        return '<div class="toast"><pre>Обновление запущено. Результат будет в секции статуса ниже. Лог: tmp/self-update.log</pre></div>'
    except Exception as e:
        audit_log(action="self-update", status="error", details=str(e)[:500])
        return f'<div class="toast toast-fail">{_html.escape(str(e))}</div>'


@app.get("/api/self-update/status")
async def self_update_status():
    try:
        r = subprocess.run(
            ["systemctl", "--user", "is-active", "lapg"],
            capture_output=True, text=True, timeout=5,
        )
        active = r.stdout.strip() == "active"
        r2 = subprocess.run(
            ["systemctl", "--user", "show", "lapg", "--property=ActiveEnterTimestamp"],
            capture_output=True, text=True, timeout=5,
        )
        started = r2.stdout.strip().replace("ActiveEnterTimestamp=", "")
    except Exception:
        active = False
        started = ""
    try:
        r3 = subprocess.run(
            ["git", "-C", str(PROJECT_DIR), "log", "-1", "--format=%h %s"],
            capture_output=True, text=True, timeout=5,
        )
        version = r3.stdout.strip()
    except Exception:
        version = ""
    return {"active": active, "started": started, "version": version}


@app.get("/tools/{name}/status")
async def tools_status(name: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return {"error": "Сервер не найден"}
    results = []
    for t in TOOLS:
        result = _ssh_quick_cmd(server, t["check"], timeout=5)
        installed = bool(result and result[0] and result[0].strip())
        version = result[0].strip() if installed else ""
        results.append({"id": t["id"], "installed": installed, "version": version})
    return {"tools": results}


@app.post("/tools/{name}/install/{tool_id}")
async def tools_install(name: str, tool_id: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}
    tool = next((t for t in TOOLS if t["id"] == tool_id), None)
    if not tool:
        return {"ok": False, "error": "Инструмент не найден"}
    result = _ssh_quick_cmd(server, tool["install"], timeout=120)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}
    stdout, stderr, rc = result
    output = (stdout + stderr).strip()
    return {"ok": rc == 0, "output": output or ("Готово" if rc == 0 else f"Ошибка (rc={rc})")}


@app.get("/api/databases", response_class=HTMLResponse)
async def api_databases():
    conn_ok = await _check_connection()
    if not conn_ok:
        return '<div class="error">Нет подключения к PostgreSQL</div>'
    db_sizes = await get_db_sizes()
    lines = ""
    for n, s in db_sizes:
        lines += (
            f'<tr><td>{n}</td><td>{_fmt_size(s)}</td>'
            f'<td style="display:flex;gap:.3rem;flex-wrap:wrap">'
            f'<button class="btn btn-sm" hx-post="/backup/{n}" '
            f'hx-target="#result" hx-swap="innerHTML">Бэкап</button>'
            f'<button class="btn btn-sm" data-action="open-update-modal" data-db="{n}">Обновить с сервера</button>'
            f'</td></tr>'
        )
    return f'<table class="table"><tr><th>БД</th><th>Размер</th><th></th></tr>{lines}</table>'


@app.get("/backups", response_class=HTMLResponse)
async def backups_page(request: Request):
    databases = []
    try:
        databases = await fetch_databases(
            host=DB_HOST, user=DB_SUPERUSER,
            password=DB_SUPERUSER_PASSWORD,
        )
    except Exception:
        pass
    backups = list_dump_files()
    return templates.TemplateResponse(request, "backups.html", {
        "databases": databases,
        "backups": [os.path.basename(b) for b in backups],
    })


@app.get("/cron", response_class=HTMLResponse)
async def cron_page(request: Request):
    return templates.TemplateResponse(request, "cron.html", {})


@app.get("/cron/list", response_class=HTMLResponse)
async def cron_list():
    content = get_crontab()
    lines = parse_lines(content)
    entries = list_project_entries(lines)
    if not entries:
        return '<div class="card"><div class="muted">Нет задач laPG в cron.</div></div>'
    rows = ""
    for i, (lineno, entry) in enumerate(entries):
        rows += (
            f"<tr>"
            f"<td>{entry}</td>"
            f'<td><button class="btn btn-sm" hx-post="/cron/del/{i}" hx-target="#cron-list" hx-swap="outerHTML">Удалить</button></td>'
            f"</tr>"
        )
    return (
        f'<div class="card" id="cron-list">'
        f'<table class="table"><tr><th>Задача</th><th></th></tr>{rows}</table>'
        f"</div>"
    )


@app.post("/cron/del/{idx}", response_class=HTMLResponse)
async def cron_delete(idx: int):
    content = get_crontab()
    lines = parse_lines(content)
    entries = list_project_entries(lines)
    if idx < 0 or idx >= len(entries):
        return await cron_list()
    entry_index, _ = entries[idx]
    lines.pop(entry_index)
    if set_crontab("".join(lines)):
        restart_cron()
    return await cron_list()


@app.post("/cron/add", response_class=HTMLResponse)
async def cron_add(
    schedule_preset: str = Form(...),
    schedule_custom: str = Form(""),
    daily_hour: int = Form(2),
    daily_minute: int = Form(0),
    weekly_day: int = Form(0),
    weekly_hour: int = Form(3),
    cmd_preset: str = Form(...),
    cmd_custom: str = Form(""),
):
    schedule = {"hourly": "0 * * * *"}.get(schedule_preset)
    if schedule_preset == "daily":
        schedule = f"{daily_minute} {daily_hour} * * *"
    elif schedule_preset == "weekly":
        schedule = f"0 {weekly_hour} * * {weekly_day}"
    elif schedule_preset == "custom":
        schedule = schedule_custom.strip()
    if not schedule:
        return '<div class="toast toast-fail">Неверное расписание</div>'

    if cmd_preset == "backup":
        command = build_backup_command(backup_all=True)
    elif cmd_preset == "backup_keep":
        command = build_backup_command(backup_all=True, keep=10)
    elif cmd_preset == "custom":
        command = cmd_custom.strip()
    else:
        return '<div class="toast toast-fail">Неверная команда</div>'
    if not command:
        return '<div class="toast toast-fail">Пустая команда</div>'

    content = get_crontab()
    lines = parse_lines(content)
    cron_line = build_cron_line(schedule, command)
    lines.append(cron_line + "\n")
    if set_crontab("".join(lines)):
        restart_cron()
    return await cron_list()


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
    audit_log(action="backup", service=db_name, details=output[:500] if output else "", status=status)
    return f'<div class="toast toast-{status}"><pre>{output}</pre></div>'


@app.post("/backup-all", response_class=HTMLResponse)
async def backup_all_route():
    from backup import backup_all as _backup_all

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ok = await _backup_all()
    output = buf.getvalue()
    status = "ok" if ok else "fail"
    audit_log(action="backup-all", details=output[:500] if output else "", status=status)
    return f'<div class="toast toast-{status}"><pre>{output}</pre></div>'


@app.get("/api/remote-dbs/{server_name}", response_class=HTMLResponse)
async def api_remote_dbs(server_name: str, local_db: str = ""):
    server = next((s for s in YAML_CFG.servers if s.name == server_name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'
    result = _ssh_run_cmd(server,
        "sudo -u postgres psql -t -A -c \"SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres') ORDER BY datname\" 2>/dev/null"
    )
    if result is None:
        return '<div class="toast toast-fail">SSH недоступен</div>'
    stdout, stderr, rc = result
    if rc != 0:
        return f'<div class="toast toast-fail"><pre>{stderr or stdout}</pre></div>'
    dbs = [line.strip() for line in stdout.strip().splitlines() if line.strip()]
    if not dbs:
        return '<div class="muted">Нет БД на сервере</div>'
    has_match = local_db in dbs
    if has_match:
        options = f'<option value="{local_db}" selected>{local_db}</option>'
        options += "".join(f'<option value="{d}">{d}</option>' for d in dbs if d != local_db)
    else:
        options = '<option value="" disabled selected>Выберите БД...</option>'
        options += "".join(f'<option value="{d}">{d}</option>' for d in dbs)
    return f'<select id="remote-db-select" class="select">{options}</select>'


@app.post("/update-from-server", response_class=HTMLResponse)
async def update_from_server(
    local_db: str = Form(...),
    server_name: str = Form(...),
    remote_db: str = Form(...),
):
    server = next((s for s in YAML_CFG.servers if s.name == server_name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'

    buf = io.StringIO()
    env = os.environ.copy()
    if DB_SUPERUSER_PASSWORD:
        env["PGPASSWORD"] = DB_SUPERUSER_PASSWORD

    def log(msg):
        print(msg, file=buf, flush=True)

    def run_in_executor(func, *args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, lambda: func(*args, **kwargs))

    log(f"=== Обновление {local_db} из {server_name}:{remote_db} ===")

    log("\n[1/5] Бэкап локальной БД...")
    cfg = get_db_backup_config(local_db, YAML_CFG)
    ok, output = await run_in_executor(_capture_output, do_backup, local_db, cfg.compress, cfg.keep)
    log(output)
    if not ok:
        log("ОШИБКА: бэкап не удался, обновление отменено.")
        return f'<div class="toast toast-fail"><pre>{buf.getvalue()}</pre></div>'

    log("\n[2/5] Дамп с удалённого сервера...")
    remote_dump = f"/tmp/_lapg_sync_{remote_db}.dump"
    cmd = (
        f"sudo -u postgres pg_dump -F c -f {remote_dump} {remote_db} 2>&1 && echo 'DUMP_OK' || echo 'DUMP_FAIL'"
    )
    result = await run_in_executor(_ssh_run_cmd, server, cmd)
    if result is None:
        log("ОШИБКА: SSH недоступен.")
        return f'<div class="toast toast-fail"><pre>{buf.getvalue()}</pre></div>'
    stdout, stderr, rc = result
    log((stdout + stderr).strip())
    if "DUMP_FAIL" in stdout or rc != 0:
        log("ОШИБКА: pg_dump на сервере не удался.")
        return f'<div class="toast toast-fail"><pre>{buf.getvalue()}</pre></div>'

    log("\n[3/5] Скачивание дампа...")
    scp_base = [
        "scp", "-O",
        "-o", "StrictHostKeyChecking=accept-new",
        "-P", str(server.port),
    ]
    if server.name in _ssh_sessions and _has_sshpass():
        scp_cmd = ["sshpass", "-p", _ssh_sessions[server.name]] + scp_base
    else:
        scp_cmd = scp_base
    scp_cmd.extend([f"{server.user}@{server.host}:{remote_dump}", remote_dump])
    try:
        r = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=60)
        if r.returncode != 0:
            log(f"ОШИБКА SCP: {r.stderr}")
            cleanup_cmd = f"rm -f {remote_dump}"
            await run_in_executor(_ssh_run_cmd, server, cleanup_cmd)
            return f'<div class="toast toast-fail"><pre>{buf.getvalue()}</pre></div>'
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        log(f"ОШИБКА SCP: {e}")
        cleanup_cmd = f"rm -f {remote_dump}"
        await run_in_executor(_ssh_run_cmd, server, cleanup_cmd)
        return f'<div class="toast toast-fail"><pre>{buf.getvalue()}</pre></div>'
    log("Дамп скачан.")

    log("\n[4/5] Восстановление локальной БД...")
    try:
        check = f"SELECT 1 FROM pg_database WHERE datname = '{local_db}'"
        result = subprocess_run_psql("psql", check)
        exists = "1" in result.stdout
    except Exception:
        exists = False

    if exists:
        log(f"Удаление {local_db}...")
        try:
            subprocess_run_psql("dropdb", local_db)
        except Exception as e:
            log(f"Ошибка удаления: {e}")
            return f'<div class="toast toast-fail"><pre>{buf.getvalue()}</pre></div>'

    log(f"Создание {local_db}...")
    try:
        subprocess_run_psql("createdb", local_db)
    except Exception as e:
        log(f"Ошибка создания: {e}")
        return f'<div class="toast toast-fail"><pre>{buf.getvalue()}</pre></div>'

    try:
        cmd = [
            "pg_restore",
            "-h", DB_HOST,
            "-U", DB_SUPERUSER,
            "-d", local_db,
            remote_dump,
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=300)
        if r.stderr:
            log(r.stderr.strip())
        if r.returncode != 0:
            log(f"Предупреждения pg_restore (rc={r.returncode}), но БД восстановлена.")
        else:
            log("Восстановление завершено успешно.")
    except Exception as e:
        log(f"Ошибка pg_restore: {e}")
        return f'<div class="toast toast-fail"><pre>{buf.getvalue()}</pre></div>'

    log("\n[5/5] Очистка...")
    cleanup_cmd = f"rm -f {remote_dump}"
    await run_in_executor(_ssh_run_cmd, server, cleanup_cmd)
    try:
        os.remove(remote_dump)
    except OSError:
        pass

    log("\n=== Готово ===")
    audit_log(
        action="update-from-server",
        server=server_name,
        service=local_db,
        details=f"remote_db={remote_db}",
        status="ok",
    )
    return f'<div class="toast toast-ok"><pre>{buf.getvalue()}</pre></div>'


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
        f'<tr class="db-row" data-path="{f["path"]}"><td>{f["path"]}</td><td>{f["size"]}</td><td>{f["mtime"]}</td>'
        f"<td><span class=\"badge badge-{'green' if f['access'] == 'редактирование' else 'red'}\">{f['access']}</span></td>"
        f'<td style="text-align:right">'
        + (
            f'<a class="btn btn-sm btn-edit" href="/sqlite/{urllib.parse.quote(name)}/browse?path={urllib.parse.quote(f["path"])}" title="редактировать данные">\U0001F589</a>'
            if f['access'] == 'редактирование' else ''
        )
        + "</td></tr>"
        for f in files
    )
    return f'<table class="table" id="db-list"><tr><th>Путь</th><th>Размер</th><th>Изменён</th><th>Доступ</th><th style="width:1px"></th></tr>{rows}</table>'


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

    col_types = col_types_json if isinstance(col_types_json, dict) else {}
    return templates.TemplateResponse(request, "sqlite_table.html", {
        "server_name": name, "db_path": path, "db_path_enc": urllib.parse.quote(path),
        "table": table, "columns": display_cols, "rows": rows,
        "bool_cols": bool_cols, "col_types": col_types, "error": error,
    })



@app.post("/sqlite/{name}/table/update-cell", response_class=HTMLResponse)
async def sqlite_update_cell(request: Request, name: str):
    import html as _html
    form = await request.form()
    path = form.get("path", "")
    table = form.get("table", "")
    rowid = form.get("rowid", "")
    column = form.get("column", "")
    value = form.get("value", "")

    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        print(f"[update-cell] Сервер '{name}' не найден")
        return '<td class="toast toast-fail">Сервер не найден</td>'

    try:
        col_types = _get_column_types(server, path, table)
        col_type = col_types.get(column, "").upper()
        is_bool = "INT" in col_type or "BOOL" in col_type

        if is_bool:
            value = "1" if value in ("1", "✅") else "0"

        cmd = (
            "python3 << 'PYEOF'\n"
            "import sqlite3\n"
            f'conn = sqlite3.connect({repr(path)})\n'
            f'conn.execute("UPDATE \\"{table}\\" SET \\"{column}\\" = ? WHERE rowid = ?", ({repr(value)}, {repr(rowid)}))\n'
            "conn.commit()\n"
            "conn.close()\n"
            "print('OK')\n"
            "PYEOF"
        )

        result = _ssh_run_cmd(server, cmd)
        if result is None:
            print(f"[update-cell] Ошибка подключения к {server.name}")
            return '<td class="toast toast-fail">Ошибка подключения</td>'
        stdout, stderr, rc = result
        if rc != 0 or stdout.strip() != "OK":
            detail = (stdout or stderr or "").strip()[:300]
            print(f"[update-cell] SSH rc={rc}: {detail}")
            return f'<td class="toast toast-fail">Ошибка: {_html.escape(detail)}</td>'

        if is_bool:
            display = "✅" if value == "1" else "❌"
            data_extra = ""
        else:
            display = _html.escape(value[:200], quote=True) + ("..." if len(value) > 200 else "")
            if len(value) > 200:
                data_extra = f'data-full="{_html.escape(value, quote=True)}" '
            else:
                data_extra = ""
        return (
            f'<td class="dbl-edit" data-path="{_html.escape(path, quote=True)}" '
            f'data-table="{_html.escape(table, quote=True)}" '
            f'data-server="{_html.escape(name, quote=True)}" '
            f'data-rowid="{_html.escape(str(rowid), quote=True)}" '
            f'data-column="{_html.escape(column, quote=True)}" '
            f'data-bool="{"1" if is_bool else "0"}" '
            f'{data_extra}'
            f'ondblclick="editCell(this)">{display}</td>'
        )
    except Exception as e:
        print(f"[update-cell] Исключение: {e}")
        return f'<td class="toast toast-fail">Ошибка: {_html.escape(str(e)[:300], quote=True)}</td>'


@app.post("/sqlite/{name}/execute-sql", response_class=HTMLResponse)
async def sqlite_execute_sql(request: Request, name: str):
    form = await request.form()
    path = form.get("path", "")
    sql = form.get("sql", "").strip()

    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'

    encoded_sql = base64.b64encode(sql.encode()).decode()
    cmd = (
        "python3 << 'PYEOF'\n"
        "import sqlite3, json, sys, base64\n"
        f'conn = sqlite3.connect("{path}")\n'
        "conn.row_factory = sqlite3.Row\n"
        f'sql = base64.b64decode("{encoded_sql}").decode()\n'
        "try:\n"
        "    c = conn.execute(sql)\n"
        "    if c.description:\n"
        "        rows = [dict(r) for r in c.fetchall()]\n"
        "        cols = [d[0] for d in c.description]\n"
        "        print(json.dumps({\"type\": \"select\", \"columns\": cols, \"rows\": rows}))\n"
        "    else:\n"
        "        conn.commit()\n"
        "        print(json.dumps({\"type\": \"execute\", \"rowcount\": c.rowcount}))\n"
        "except Exception as e:\n"
        "    print(json.dumps({\"type\": \"error\", \"message\": str(e)}), file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "finally:\n"
        "    conn.close()\n"
        "PYEOF"
    )

    result = _ssh_run_cmd(server, cmd)
    if result is None:
        return '<div class="toast toast-fail">Ошибка подключения</div>'

    stdout, stderr, rc = result
    if rc != 0:
        try:
            err = json.loads(stderr.strip())
            return f'<div class="toast toast-fail">{err.get("message", stderr[:500])}</div>'
        except json.JSONDecodeError:
            return f'<div class="toast toast-fail">{stderr[:500]}</div>'

    if not stdout.strip():
        return '<div class="toast toast-fail">Пустой ответ</div>'

    data = json.loads(stdout.strip())
    if data.get("type") == "select":
        cols = data.get("columns", [])
        rows = data.get("rows", [])
        html = '<table class="table"><tr>'
        for c in cols:
            html += f"<th>{c}</th>"
        html += "</tr>"
        for r in rows:
            html += "<tr>"
            for c in cols:
                val = r.get(c)
                html += f"<td>{val if val is not None else 'NULL'}</td>"
            html += "</tr>"
        html += "</table>"
        html += f'<p class="muted">{len(rows)} rows</p>'
        return html
    elif data.get("type") == "execute":
        rc_count = data.get("rowcount", 0)
        if rc_count < 0:
            return '<div class="toast toast-ok">OK</div>'
        return f'<div class="toast toast-ok">OK, {rc_count} rows affected</div>'

    return '<div class="toast toast-fail">Неизвестный ответ</div>'


@app.get("/compare", response_class=HTMLResponse)
async def compare_page(request: Request):
    try:
        dbs = await fetch_databases(host=DB_HOST, user=DB_SUPERUSER, password=DB_SUPERUSER_PASSWORD)
    except (ConnectionError, asyncpg.PostgresError, asyncio.TimeoutError):
        dbs = []
    return templates.TemplateResponse(request, "compare.html", {"dbs": dbs})


@app.post("/compare/run", response_class=HTMLResponse)
async def compare_run(request: Request, db_a: str = Form(...), db_b: str = Form(...), mode: str = Form(...)):
    if db_a == db_b:
        return '<div class="toast toast-fail">Выбрана одна и та же база</div>'

    try:
        schema_a, schema_b = await asyncio.gather(
            _fetch_schema(DB_HOST, DB_SUPERUSER, DB_SUPERUSER_PASSWORD, db_a),
            _fetch_schema(DB_HOST, DB_SUPERUSER, DB_SUPERUSER_PASSWORD, db_b),
        )
    except (asyncpg.PostgresError, asyncio.TimeoutError) as e:
        return f'<div class="toast toast-fail">Ошибка: {e}</div>'

    _normalize_schema(schema_a)
    _normalize_schema(schema_b)

    diffs = build_diffs(db_a, schema_a, db_b, schema_b)

    if mode == "2":
        diffs = [d for d in diffs if not d["column"]]
    elif mode == "3":
        diffs = [d for d in diffs if d["column"]]
    else:
        diffs = diffs

    if not diffs:
        return '<div class="compare-empty">Схемы полностью совпадают</div>'

    rows = []
    for d in diffs:
        col = d.get("column", "") or ""
        rows.append(f"""<tr class="diff-row" onclick="toggleDiff(this)">
  <td>{d["table"]}</td><td>{col}</td><td>{d["description"]}</td>
</tr>
<tr class="diff-sql" style="display:none">
  <td colspan="3">
    <div class="diff-sql-block">
      <strong>→ {db_b}:</strong>
      <pre><code>{d["sql_to_b"]}</code></pre>
    </div>
    <div class="diff-sql-block">
      <strong>→ {db_a}:</strong>
      <pre><code>{d["sql_to_a"]}</code></pre>
    </div>
  </td>
</tr>""")

    return f"""<table class="table diff-table">
<thead><tr><th>Таблица</th><th>Колонка</th><th>Различие</th></tr></thead>
<tbody>
{''.join(rows)}
</tbody>
</table>
<script>
function toggleDiff(el) {{
  var sqlRow = el.nextElementSibling;
  if (sqlRow && sqlRow.classList.contains('diff-sql')) {{
    sqlRow.style.display = sqlRow.style.display === 'none' ? '' : 'none';
  }}
}}
</script>"""


def _detect_tech(exec_start):
    if not exec_start:
        return ""
    exe = exec_start.lower()
    if "python" in exe:
        return "Python"
    if "node" in exe:
        return "Node.js"
    if "java" in exe:
        return "Java"
    if "go" in exe or "golang" in exe:
        return "Go"
    if "ruby" in exe:
        return "Ruby"
    if "php" in exe:
        return "PHP"
    if ".sh" in exe or "/bash" in exe or "/sh " in exe:
        return "Shell"
    return ""


def _clean_exec(raw):
    """Из path=/usr/bin/python3 ; argv[]=/usr/bin/python3 bot.py … извлекаем /usr/bin/python3 bot.py"""
    import re
    m = re.search(r"argv\[\]=([^;]+)", raw)
    if m:
        return m.group(1).strip()
    m = re.search(r"path=(\S+)", raw)
    return m.group(1) if m else raw[:120]


def _clean_uptime(raw):
    """'Thu 2026-07-02 01:11:47 MSK' → '2026-07-02 01:11'"""
    import re
    m = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", raw)
    return m.group(0) if m else raw


def _parse_svc_props(text):
    info = {"desc": "", "state": "", "sub": "", "memory": "", "uptime": "",
            "exec_start": "", "type": "", "user": "", "fragment_path": "",
            "working_dir": "", "tech": ""}
    for line in text.splitlines():
        k, _, v = line.partition("=")
        if k == "Description":
            info["desc"] = v
        elif k == "ActiveState":
            info["state"] = v
        elif k == "SubState":
            info["sub"] = v
        elif k == "MemoryCurrent":
            info["memory"] = _fmt_size(int(v)) if v and v.isdigit() else ""
        elif k == "ExecMainStartTimestamp":
            info["uptime"] = _clean_uptime(v) if v and v != "(n/a)" else ""
        elif k == "ExecStart":
            info["exec_start"] = _clean_exec(v)
        elif k == "Type":
            info["type"] = v
        elif k == "User":
            info["user"] = v
        elif k == "FragmentPath":
            info["fragment_path"] = v
        elif k == "WorkingDirectory":
            info["working_dir"] = v
    info["tech"] = _detect_tech(info["exec_start"])
    return info


@app.get("/servers/{name}/users", response_class=HTMLResponse)
async def server_users(request: Request, name: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return HTMLResponse("Сервер не найден", status_code=404)

    # get all users (single SSH call)
    result = _ssh_run_cmd(server, "getent passwd | awk -F: '$3>=100 && $3!=65534{print $1\":\"$3\":\"$4\":\"$6\":\"$7}'")
    if not result or not result[0].strip():
        return templates.TemplateResponse(request, "users.html", {"server": server, "users": []})

    raw_users = [l.strip() for l in result[0].strip().split("\n") if l.strip()]

    # get groups + passwd status for all users (single SSH call)
    usernames = " ".join(u.split(":")[0] for u in raw_users)
    info_cmd = f"""
for u in {usernames}; do
  g=$(groups $u 2>/dev/null)
  p=$(passwd -S $u 2>/dev/null)
  echo "$u|$g|$p"
done
"""
    info_result = _ssh_run_cmd(server, info_cmd.strip())
    info_map = {}
    if info_result and info_result[0].strip():
        for line in info_result[0].strip().split("\n"):
            parts = line.strip().split("|", 2)
            if len(parts) == 3:
                info_map[parts[0]] = (parts[1], parts[2])

    users = []
    for line in raw_users:
        fields = line.split(":")
        if len(fields) >= 5:
            u = {"username": fields[0], "uid": fields[1], "gid": fields[2], "home": fields[3], "shell": fields[4]}
            u["groups"] = ""
            u["locked"] = False
            if fields[0] in info_map:
                g, p = info_map[fields[0]]
                if g:
                    gg = g.split(":", 1)
                    u["groups"] = gg[-1].strip() if len(gg) > 1 else g
                if p:
                    st = p.strip().split()
                    u["locked"] = len(st) >= 2 and st[1] == "L"
            users.append(u)

    return templates.TemplateResponse(request, "users.html", {"server": server, "users": users})


@app.get("/servers/{name}/aliases", response_class=HTMLResponse)
async def server_aliases(request: Request, name: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return HTMLResponse("Сервер не найден", status_code=404)

    rc_files = ["/root/.bashrc", "/root/.bash_aliases", "/root/.bash_profile"]
    cat_cmd = "; ".join(
        f'echo "###FILE:{f}"; cat {f} 2>/dev/null' for f in rc_files
    )
    result = _ssh_run_cmd(server, cat_cmd)
    if result is None:
        return templates.TemplateResponse(request, "aliases.html", {
            "server": server, "aliases": [], "files": [], "error": "SSH недоступен",
        })

    out = result[0] or ""
    files = []
    aliases = []
    cur_file = ""
    buf = []
    for line in out.splitlines():
        if line.startswith("###FILE:"):
            if cur_file:
                files.append({"path": cur_file, "content": "\n".join(buf)})
            cur_file = line[len("###FILE:"):].strip()
            buf = []
        else:
            buf.append(line)
    if cur_file:
        files.append({"path": cur_file, "content": "\n".join(buf)})

    for f in files:
        for line in f["content"].splitlines():
            m = re.match(r"\s*alias\s+([A-Za-z0-9_.\-]+)=(.*)$", line)
            if m:
                cmd = m.group(2).strip()
                if len(cmd) >= 2 and cmd[0] in "'\"" and cmd[-1] == cmd[0]:
                    cmd = cmd[1:-1]
                aliases.append({"name": m.group(1), "cmd": cmd, "file": f["path"]})

    return templates.TemplateResponse(request, "aliases.html", {
        "server": server, "aliases": aliases, "files": [f for f in files if f["content"].strip()], "error": "",
    })


@app.post("/servers/{name}/users/{username}/toggle-lock", response_class=HTMLResponse)
async def user_toggle_lock(name: str, username: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'

    # check current state
    ps = _ssh_run_cmd(server, f"passwd -S {username} 2>/dev/null")
    locked = False
    if ps and ps[0]:
        st = ps[0].strip().split()
        locked = len(st) >= 2 and st[1] == "L"

    action = "usermod -U" if locked else "sudo usermod -L"
    result = _ssh_run_cmd(server, f"{action} {username} 2>&1")
    if result is None:
        return '<div class="toast toast-fail">SSH недоступен</div>'
    stdout, stderr, rc = result
    if rc == 0:
        new_state = "разблокирован" if locked else "заблокирован"
        return f'<div class="toast toast-ok">Пользователь {username} {new_state}</div>'
    else:
        return f'<div class="toast toast-fail">{(stderr or stdout).strip()}</div>'


@app.post("/api/exec")
async def api_exec(request: Request):
    import json as _json
    try:
        body = _json.loads(await request.body())
    except Exception:
        return {"error": "Invalid JSON"}

    server_name = body.get("server", "")
    cwd = body.get("cwd", "")
    command = body.get("command", "")

    server = next((s for s in YAML_CFG.servers if s.name == server_name), None)
    if not server:
        return {"error": "Сервер не найден"}
    if not command:
        return {"error": "Пустая команда"}

    full_cmd = f"cd {cwd} 2>/dev/null; " + command if cwd else command
    result = _ssh_run_cmd(server, full_cmd)
    if result is None:
        return {"error": "SSH недоступен"}
    stdout, stderr, rc = result
    return {"output": stdout + stderr, "rc": rc}


@app.get("/bots", response_class=HTMLResponse)
async def bots_page(request: Request):
    return templates.TemplateResponse(request, "bots.html", {"servers": YAML_CFG.servers})


@app.get("/bots/services", response_class=HTMLResponse)
async def services_list(request: Request):
    return templates.TemplateResponse(request, "services.html", {"services": list_services()})



@app.get("/bots/services/{service_id}/deploy", response_class=HTMLResponse)
async def service_deploy(request: Request, service_id: int):
    s = get_service(service_id)
    if not s:
        return HTMLResponse("Сервис не найден", status_code=404)
    return templates.TemplateResponse(request, "deploy_actions.html", {"s": s})


def _deploy_paths(s):
    raw_base = s.get("base_path") or "/home/project/bots/"
    svc_name = s.get("project_name", "")
    dir_name = svc_name.replace(".service", "").replace("-", "_")
    base_last = raw_base.rstrip("/").rsplit("/", 1)[-1] if "/" in raw_base.rstrip("/") else ""
    if base_last.replace("-", "_") == dir_name and base_last:
        base = raw_base.rstrip("/").rsplit("/", 1)[0] + "/"
        target = dir_name
        full_path = (base + dir_name).rstrip("/")
    else:
        base = raw_base.rstrip("/") + "/"
        target = dir_name
        full_path = (base + dir_name).rstrip("/")
    return base, dir_name, target, full_path


@app.get("/bots/services/{service_id}/deploy/repo-status")
async def repo_status(service_id: int):
    import json as _json
    s = get_service(service_id)
    if not s:
        return {"error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"error": "Сервер не найден в конфиге"}

    base, dir_name, target, full_path = _deploy_paths(s)

    cmd = f"test -d {full_path} && echo EXISTS || echo NOT_FOUND"
    result = _ssh_quick_cmd(server, cmd, timeout=6)
    if result is None:
        return {"error": "SSH недоступен"}

    exists = "EXISTS" in (result[0] or "")
    url = s.get("repo_url", "")
    if url:
        clone_cmd = f"cd {base} && git clone {url} {target}"
    else:
        clone_cmd = "(URL репозитория не указан)"

    return {"exists": exists, "dir": full_path, "dir_name": dir_name, "base": base, "command": clone_cmd}


@app.post("/bots/services/{service_id}/deploy/git-clone")
async def git_clone(service_id: int):
    return await _run_git(service_id, pull=False)


@app.post("/bots/services/{service_id}/deploy/git-pull")
async def git_pull(service_id: int):
    return await _run_git(service_id, pull=True)


async def _run_git(service_id: int, pull: bool):
    import json as _json
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}

    base, dir_name, target, full_path = _deploy_paths(s)
    url = s.get("repo_url", "")

    if not url:
        return {"ok": False, "error": "URL репозитория не указан"}

    if pull:
        cmd = (
            f"git config --global --add safe.directory {full_path} 2>/dev/null; "
            f"cd {full_path} && git pull 2>&1"
        )
    else:
        cmd = f"cd {base} && git clone {url} {target} 2>&1"

    result = _ssh_quick_cmd(server, cmd, timeout=30)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}

    stdout, stderr, rc = result
    output = (stdout + stderr).strip()
    if rc == 0:
        return {"ok": True, "output": output or "Готово"}
    else:
        return {"ok": False, "error": output or f"Ошибка (rc={rc})"}


@app.post("/bots/services/{service_id}/check-perms")
async def check_perms(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}
    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}
    _, _, _, full_path = _deploy_paths(s)
    user = (s.get("systemd_user") or s.get("project_name", "")).strip()
    exclude = "! -path '*/.git/*' ! -path '*/venv/*'"

    def try_sudo(test_flag):
        cmd = f"sudo -u {user} find {full_path} {test_flag} {exclude} 2>&1 | head -50"
        r = _ssh_quick_cmd(server, cmd, timeout=15)
        if r is None or r[2] == 255:
            return None
        return (r[0] + r[1]).strip()

    def try_fallback(test_flag):
        cmd = f"find {full_path} {test_flag} {exclude} 2>/dev/null | head -50"
        r = _ssh_quick_cmd(server, cmd, timeout=15)
        if r is None or r[2] == 255:
            return None
        return (r[0] + r[1]).strip()

    unreadable = try_sudo("-not -readable")
    if unreadable is None:
        unreadable = try_fallback("! -perm /o+r")
        warn_read = "sudo недоступен, проверка по world-битам"
    else:
        warn_read = None

    unwritable = try_sudo("-type d -not -writable")
    if unwritable is None:
        unwritable = try_fallback("-type d ! -perm /o+w")
        warn_write = "sudo недоступен, проверка по world-битам"
    else:
        warn_write = None

    if unreadable is None and unwritable is None:
        return {"ok": False, "error": "SSH недоступен"}

    lines = []
    if warn_read:
        lines.append(f"⚠ {warn_read}")
    if warn_write:
        lines.append(f"⚠ {warn_write}")

    if unreadable:
        lines.append(f"❌ Нет доступа на чтение ({user}):\n{unreadable}")
    if unwritable:
        lines.append(f"❌ Нет доступа на запись — директории ({user}):\n{unwritable}")

    if not unreadable and not unwritable:
        if warn_read or warn_write:
            msg = "✅ Все права в порядке"
            if warn_read:
                msg += f"\n⚠ {warn_read}"
            if warn_write:
                msg += f"\n⚠ {warn_write}"
            return {"ok": True, "output": msg}
        return {"ok": True, "output": f"✅ Все права в порядке для пользователя {user}"}

    return {"ok": True, "output": "\n\n".join(lines)}


@app.post("/bots/services/{service_id}/fix-perms")
async def fix_perms(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}
    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}
    _, _, _, full_path = _deploy_paths(s)
    user = (s.get("systemd_user") or s.get("project_name", "")).strip()
    exclude = "! -path '*/.git/*' ! -path '*/venv/*'"
    cmd = (
        f"echo '--- chown {user}:{user} ---' && "
        f"sudo chown -R {user}:{user} {full_path} 2>&1 && "
        f"echo '--- chmod u+wx на директории ---' && "
        f"find {full_path} -type d {exclude} -exec chmod u+wx {{}} \\; 2>&1 && "
        f"echo 'Готово'"
    )
    result = _ssh_quick_cmd(server, cmd, timeout=60)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}
    stdout, stderr, rc = result
    output = (stdout + stderr).strip()
    if rc == 0:
        audit_log(
            action="fix-perms",
            server=s.get("server_name", ""),
            service=s.get("project_name", ""),
            details=f"user={user}",
            status="ok",
        )
        return {"ok": True, "output": f"Права исправлены для пользователя {user}:\n{output}"}
    return {"ok": False, "error": output or f"Ошибка (rc={rc})"}


@app.get("/bots/services/{service_id}/deploy/venv-status")
async def venv_status(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"error": "Сервер не найден"}

    _, _, _, full_path = _deploy_paths(s)
    cmd = f"test -f {full_path}/venv/bin/python && echo EXISTS || echo NOT_FOUND"
    result = _ssh_quick_cmd(server, cmd, timeout=6)
    if result is None:
        return {"error": "SSH недоступен"}

    exists = "EXISTS" in (result[0] or "")
    available = []
    if not exists:
        avail_result = _ssh_quick_cmd(
            server,
            r'for v in python3 python3.10 python3.11 python3.12 python3.13 python3.14; do c=$(command -v "$v" 2>/dev/null) && [ -n "$c" ] && vv=$("$v" --version 2>&1) && echo "$v|$vv"; done',
            timeout=6
        )
        seen = set()
        if avail_result and avail_result[0]:
            for line in avail_result[0].strip().split("\n"):
                line = line.strip()
                if "|" not in line:
                    continue
                binary, vv = line.split("|", 1)
                vv = vv.strip()
                if vv and vv.startswith("Python ") and vv not in seen:
                    seen.add(vv)
                    available.append({"binary": binary, "version": vv})
    return {"exists": exists, "dir": full_path + "/venv", "available": available}


@app.post("/bots/services/{service_id}/deploy/venv-create")
async def venv_create(service_id: int, body: dict = None):
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}

    python_bin = (body or {}).get("python_bin", "python3")

    _, _, _, full_path = _deploy_paths(s)
    cmd = f"cd {full_path} && {python_bin} -m venv venv && venv/bin/pip install --upgrade pip 2>&1"
    result = _ssh_quick_cmd(server, cmd, timeout=30)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}

    stdout, stderr, rc = result
    output = (stdout + stderr).strip()
    if rc == 0:
        return {"ok": True, "output": output or "Готово"}
    else:
        return {"ok": False, "error": output or f"Ошибка (rc={rc})"}


@app.post("/bots/services/{service_id}/deploy/venv-remove")
async def venv_remove(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}
    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}
    _, _, _, full_path = _deploy_paths(s)
    result = _ssh_quick_cmd(server, f"rm -rf {full_path}/venv && echo OK", timeout=10)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}
    stdout, stderr, rc = result
    if rc == 0:
        return {"ok": True, "output": "venv удалён"}
    else:
        return {"ok": False, "error": (stdout + stderr).strip() or f"Ошибка (rc={rc})"}


@app.get("/bots/services/{service_id}/deploy/pyver-status")
async def pyver_status(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"error": "Сервер не найден"}

    _, _, _, full_path = _deploy_paths(s)

    ver_result = _ssh_quick_cmd(server, f"{full_path}/venv/bin/python --version 2>&1", timeout=4)
    current = ver_result[0].strip() if ver_result and ver_result[0] else ""

    avail_result = _ssh_quick_cmd(
        server,
        r'for v in python3 python3.10 python3.11 python3.12 python3.13 python3.14; do c=$(command -v "$v" 2>/dev/null) && [ -n "$c" ] && vv=$("$v" --version 2>&1) && echo "$v|$vv"; done',
        timeout=6
    )
    available = []
    seen = set()
    if avail_result and avail_result[0]:
        for line in avail_result[0].strip().split("\n"):
            line = line.strip()
            if "|" not in line:
                continue
            _, vv = line.split("|", 1)
            vv = vv.strip()
            if vv and vv.startswith("Python ") and vv not in seen:
                seen.add(vv)
                available.append(vv)

    return {"current_version": current, "available": available}


@app.post("/bots/services/{service_id}/deploy/pyver-switch")
async def pyver_switch(service_id: int, body: dict = None):
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}

    version = (body or {}).get("version", "")
    # extract python binary name: "Python 3.12.3" -> "python3.12"
    parts = version.replace("Python ", "").split(".")
    if len(parts) < 2:
        return {"ok": False, "error": "Неверный формат версии"}
    py_bin = f"python{parts[0]}.{parts[1]}"

    _, _, _, full_path = _deploy_paths(s)

    # check binary exists
    check = _ssh_quick_cmd(server, f"command -v {py_bin} 2>&1", timeout=4)
    if not check or not check[0]:
        return {"ok": False, "error": f"{py_bin} не найден на сервере"}

    cmd = f"rm -rf {full_path}/venv && {py_bin} -m venv {full_path}/venv && {full_path}/venv/bin/pip install --upgrade pip 2>&1"
    result = _ssh_quick_cmd(server, cmd, timeout=30)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}

    stdout, stderr, rc = result
    output = (stdout + stderr).strip()
    action_name = "git-pull" if pull else "git-clone"
    status = "ok" if rc == 0 else "error"
    audit_log(
        action=action_name,
        server=s.get("server_name", ""),
        service=s.get("project_name", ""),
        details=output[:500] if output else "",
        status=status,
    )
    if rc == 0:
        return {"ok": True, "output": output or "Готово"}
    else:
        return {"ok": False, "error": output or f"Ошибка (rc={rc})"}


@app.get("/bots/services/{service_id}/deploy/deps-status")
async def deps_status(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"error": "Сервер не найден"}

    _, _, _, full_path = _deploy_paths(s)
    files = ["requirements.txt", "pyproject.toml", "setup.py", "setup.cfg"]
    found = []
    for f in files:
        result = _ssh_quick_cmd(server, f"test -f {full_path}/{f} && echo 1", timeout=4)
        if result and result[0] and "1" in result[0]:
            found.append(f)

    return {"files": found}


@app.post("/bots/services/{service_id}/deploy/deps-install")
async def deps_install(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}

    _, _, _, full_path = _deploy_paths(s)
    cmd = f"cd {full_path} && if [ -f requirements.txt ]; then venv/bin/pip install -r requirements.txt; elif [ -f pyproject.toml ]; then venv/bin/pip install -e .; elif [ -f setup.py ]; then venv/bin/pip install -e .; elif [ -f setup.cfg ]; then venv/bin/pip install -e .; else echo 'Файлы зависимостей не найдены'; fi 2>&1"
    result = _ssh_quick_cmd(server, cmd, timeout=60)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}

    stdout, stderr, rc = result
    output = (stdout + stderr).strip()
    if rc == 0:
        return {"ok": True, "output": output or "Готово"}
    else:
        return {"ok": False, "error": output or f"Ошибка (rc={rc})"}


def _service_filename(s):
    """Generate .service filename from project_name."""
    name = s.get("project_name", "app").strip().lower().replace(" ", "-")
    name = "".join(c for c in name if c.isalnum() or c in "-_.")
    if name.endswith(".service"):
        name = name[:-8]
    return name.replace("_", "-") + ".service"


def _generate_service_content(s, full_path):
    """Generate systemd .service file content from service record."""
    filename = _service_filename(s)
    name = filename.replace(".service", "")
    desc = (s.get("description") or "").strip() or f"{s.get('project_name', 'App')} service"
    user = (s.get("systemd_user") or "root").strip()
    ep = (s.get("entry_point") or "").strip()
    if ep.startswith("/"):
        ep_path = ep
    else:
        ep_path = f"{full_path}/{ep}"
    exec_start = f"{full_path}/venv/bin/python3 -u {ep_path}"
    port = (s.get("port") or "").strip()
    env = (s.get("extra_env") or "").strip()

    lines = [
        "[Unit]",
        f"Description={desc}",
        "After=network.target",
        "",
        "[Service]",
        "Type=simple",
        f"User={user}",
        f"Group={user}",
        f"WorkingDirectory={full_path}",
        f"ExecStart={exec_start}",
        "Restart=always",
        "RestartSec=10",
        "StandardOutput=journal",
        "StandardError=journal",
    ]
    if port:
        lines.append(f"Environment=PORT={port}")
    if env:
        for e_line in env.split("\n"):
            e_line = e_line.strip()
            if e_line:
                lines.append(f"Environment={e_line}")
    lines.extend(["", "[Install]", "WantedBy=multi-user.target"])
    return "\n".join(lines) + "\n", filename


def _service_username(s):
    """Generate system username from project_name."""
    return _service_filename(s).replace(".service", "")


@app.get("/bots/services/{service_id}/deploy/user-status")
async def user_status(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"error": "Сервер не найден"}

    _, _, _, full_path = _deploy_paths(s)
    username = _service_username(s)

    check = _ssh_quick_cmd(server, f"id -u {username} 2>/dev/null && echo 1 || echo 0", timeout=4)
    exists = check and "1" in (check[0] or "")

    return {"exists": exists, "username": username, "full_path": full_path}


@app.post("/bots/services/{service_id}/deploy/user-create")
async def user_create(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}

    _, _, _, full_path = _deploy_paths(s)
    username = _service_username(s)

    cmd = (
        f"sudo useradd --system --no-create-home --shell /usr/sbin/nologin {username} 2>&1; "
        f"sudo chown -R {username}:{username} {full_path} 2>&1; "
        f"sudo chmod -R 750 {full_path} 2>&1; echo OK"
    )
    result = _ssh_quick_cmd(server, cmd, timeout=15)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}

    stdout, stderr, rc = result
    output = (stdout + stderr).strip()
    if rc == 0 and "OK" in output:
        return {"ok": True, "output": f"Пользователь {username} создан, права установлены"}
    else:
        return {"ok": True, "output": output or f"Пользователь {username} создан, права установлены"}


@app.get("/bots/services/{service_id}/deploy/service-status")
async def service_status(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"error": "Сервер не найден"}

    _, _, _, full_path = _deploy_paths(s)
    content, filename = _generate_service_content(s, full_path)
    svc_path = f"/etc/systemd/system/{filename}"

    # check if file exists and read it
    check = _ssh_quick_cmd(server, f"test -f {svc_path} && echo 1 || echo 0", timeout=4)
    exists = check and "1" in (check[0] or "")

    remote_content = ""
    if exists:
        cat_result = _ssh_quick_cmd(server, f"cat {svc_path}", timeout=4)
        if cat_result and cat_result[0]:
            remote_content = cat_result[0].strip()

    return {
        "exists": exists,
        "filename": filename,
        "service_path": svc_path,
        "remote_content": remote_content,
        "generated_content": content,
        "has_entry_point": bool(s.get("entry_point", "").strip()),
    }


@app.post("/bots/services/{service_id}/deploy/service-upload")
async def service_upload(service_id: int):
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}

    _, _, _, full_path = _deploy_paths(s)
    content, filename = _generate_service_content(s, full_path)
    svc_path = f"/etc/systemd/system/{filename}"

    if not s.get("entry_point", "").strip():
        return {"ok": False, "error": "Не указана точка входа (entry_point)"}

    # upload via base64 to avoid escaping issues
    import base64 as _b64
    encoded = _b64.b64encode(content.encode()).decode()
    cmd = (
        f"echo '{encoded}' | base64 -d | sudo tee {svc_path} >/dev/null 2>&1 && "
        f"sudo systemctl daemon-reload 2>&1"
    )
    result = _ssh_quick_cmd(server, cmd, timeout=15)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}

    stdout, stderr, rc = result
    output = (stdout + stderr).strip()
    if rc == 0:
        return {"ok": True, "output": output or f"Файл {filename} загружен"}
    else:
        return {"ok": False, "error": output or f"Ошибка (rc={rc})"}


@app.get("/bots/services/{service_id}/deploy/ctl/{action}")
async def service_ctl(service_id: int, action: str):
    """Execute systemctl action: status, logs, daemon-reload, enable-start, restart, stop."""
    s = get_service(service_id)
    if not s:
        return {"ok": False, "error": "Сервис не найден"}

    server = next((sv for sv in YAML_CFG.servers if sv.name == s.get("server_name")), None)
    if not server:
        return {"ok": False, "error": "Сервер не найден"}

    _, _, _, full_path = _deploy_paths(s)
    filename = _service_filename(s)
    svc_path = f"/etc/systemd/system/{filename}"
    name = filename.replace(".service", "")

    action_map = {
        "status": f"systemctl status {name} 2>&1 || true",
        "logs": f"journalctl -u {name} -n 50 --no-pager 2>&1 || true",
        "daemon-reload": f"sudo systemctl daemon-reload 2>&1 && echo OK",
        "enable-start": f"sudo systemctl enable {name} 2>&1 && sudo systemctl start {name} 2>&1 && echo OK",
        "restart": f"sudo systemctl restart {name} 2>&1 && echo OK",
        "stop": f"sudo systemctl stop {name} 2>&1 && echo OK",
    }
    cmd = action_map.get(action)
    if not cmd:
        return {"ok": False, "error": f"Неизвестное действие: {action}"}

    timeout = 15 if action in ("status", "logs") else 30
    result = _ssh_quick_cmd(server, cmd, timeout=timeout)
    if result is None:
        return {"ok": False, "error": "SSH недоступен"}

    stdout, stderr, rc = result
    output = (stdout + stderr).strip()
    ok = rc == 0 or action in ("status", "logs")

    if action == "status" and "active (running)" in output:
        update_service(service_id, status="deployed")

    if action in ("daemon-reload", "enable-start", "restart", "stop"):
        audit_log(
            action=f"service:{action}",
            server=s.get("server_name", ""),
            service=s.get("project_name", ""),
            details=output[:500] if output else "",
            status="ok" if ok else "error",
        )

    return {"ok": ok, "output": output or ("" if ok else f"Ошибка (rc={rc})")}


@app.post("/bots/services/{service_id}/deploy/ctl/{action}")
async def service_ctl_post(service_id: int, action: str):
    """POST variant for actions that modify state."""
    return await service_ctl(service_id, action)


def _ssh_quick_cmd(server, cmd, timeout=8):
    """Run a quick SSH command (no fallback). Returns (stdout, stderr, rc) or None."""
    import subprocess as _sp
    base = [
        "ssh", "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=4",
        "-o", "StrictHostKeyChecking=accept-new",
        "-p", str(server.port), f"{server.user}@{server.host}",
    ]
    try:
        r = _sp.run(base + [cmd], capture_output=True, text=True, timeout=timeout)
        if r.returncode != 255:
            return r.stdout, r.stderr, r.returncode
    except (_sp.TimeoutExpired, _sp.FileNotFoundError):
        pass

    if server.name in _ssh_sessions and _has_sshpass():
        try:
            pw = _ssh_sessions[server.name]
            r = _sp.run(
                ["sshpass", "-p", pw] + base + [cmd],
                capture_output=True, text=True, timeout=timeout,
            )
            return r.stdout, r.stderr, r.returncode
        except (_sp.TimeoutExpired, _sp.FileNotFoundError):
            pass
    return None


@app.post("/bots/services/{service_id}/fetch-entrypoint")
async def fetch_entrypoint(service_id: int):
    import traceback as _tb
    print(f"[fetch-entrypoint] called for service_id={service_id}")
    try:
        svc = get_service(service_id)
        if not svc or not svc.get("server_name") or not svc.get("project_name"):
            return {}

        server = next((s for s in YAML_CFG.servers if s.name == svc["server_name"]), None)
        if not server:
            return {}

        service_name = svc["project_name"]
        if service_name.endswith(".service"):
            service_name = service_name[:-8]

        ping = _ssh_quick_cmd(server, "echo OK", timeout=5)
        if ping is None or (ping[0] and ping[0].strip() != "OK"):
            return {}

        result = _ssh_quick_cmd(server,
            f"cat /etc/systemd/system/{service_name}.service 2>/dev/null",
            timeout=8,
        )
        if result is None:
            return {}

        stdout, _, _ = result
        content = stdout.strip()

        data = {"_raw_content": content or ""}
        if not content:
            print(f"[fetch-entrypoint] .service file empty or missing")
            return data

        for line in content.split("\n"):
            line = line.strip()
            if line.startswith("ExecStart="):
                val = line.removeprefix("ExecStart=")
                parts = val.split()
                script = None
                for p in parts:
                    if p.endswith(".py"):
                        script = p
                        break
                if not script:
                    script = parts[-1] if len(parts) > 1 else val
                if "/" in script:
                    idx = script.rfind("/")
                    data["base_path"] = script[:idx+1]
                    data["entry_point"] = script[idx+1:]
                else:
                    data["entry_point"] = script
                break

        for line in content.split("\n"):
            line = line.strip()
            if line.startswith("WorkingDirectory="):
                wd = line.removeprefix("WorkingDirectory=")
                if wd:
                    if not wd.endswith("/"):
                        wd += "/"
                    data["base_path"] = wd
                break

        for line in content.split("\n"):
            line = line.strip()
            if line.startswith("User="):
                user = line.removeprefix("User=").strip()
                if user:
                    data["systemd_user"] = user
                break

        # Parse Environment= for PORT
        for line in content.split("\n"):
            line = line.strip()
            if line.startswith("Environment="):
                env_val = line.removeprefix("Environment=")
                for pair in env_val.split():
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        if "PORT" in k.upper():
                            data["port"] = v.strip()
                            break
                if "port" in data:
                    break

        # Also try to find port in ExecStart arguments
        if "port" not in data:
            for line in content.split("\n"):
                line = line.strip()
                if line.startswith("ExecStart="):
                    val = line.removeprefix("ExecStart=")
                    for arg in val.split():
                        if arg.isdigit() and 1024 <= int(arg) <= 65535:
                            data["port"] = arg
                            break
                    break

        # Parse git remote URL from project directory
        base_path = data.get("base_path", svc.get("base_path", ""))
        if base_path:
            result2 = _ssh_quick_cmd(server,
                f"git -C {base_path} remote get-url origin 2>/dev/null",
                timeout=6,
            )
            if result2 and result2[0]:
                url = result2[0].strip()
                if url:
                    data["repo_url"] = url

        data["_raw_content"] = content
        print(f"[fetch-entrypoint] data keys={list(data.keys())}")
        return data

    except Exception as e:
        print(f"[fetch-entrypoint] EXCEPTION: {e}")
        _tb.print_exc()
        return {}


@app.get("/bots/deploy", response_class=HTMLResponse)
async def deploy_page(request: Request, edit: int | None = None):
    svc = get_service(edit) if edit else None
    return templates.TemplateResponse(request, "deploy.html", {"servers": YAML_CFG.servers, "svc": svc})


@app.post("/bots/deploy/start", response_class=HTMLResponse)
async def deploy_start(request: Request):
    import json as _json
    form = await request.form()
    server = form.get("server", "").strip()
    service_name = form.get("service_name", "").strip()
    if not server or not service_name:
        return '<div class="toast toast-fail">Сервер и имя сервиса обязательны</div>'

    repo_url = form.get("repo_url", "").strip() or None
    base_path = form.get("base_path", "").strip() or "/home/project/bots/"
    systemd_user = form.get("systemd_user", "").strip() or None
    port_raw = form.get("port", "").strip()
    port = int(port_raw) if port_raw else None
    entry_point = form.get("entry_point", "").strip() or None
    git_branch = form.get("git_branch", "").strip() or "main"
    description = form.get("description", "").strip() or None
    extra_env_raw = form.get("extra_env", "").strip()
    extra_env = extra_env_raw if extra_env_raw else None
    if extra_env:
        try:
            _json.loads(extra_env)
        except _json.JSONDecodeError:
            return '<div class="toast toast-fail">Переменные окружения: неверный JSON</div>'

    db_type = form.get("db_type", "").strip() or None
    db_name = form.get("db_name", "").strip() or None

    edit_id = form.get("edit_id", "").strip()

    try:
        if edit_id:
            edit_id = int(edit_id)
            update_service(
                edit_id,
                server_name=server,
                project_name=service_name,
                repo_url=repo_url,
                base_path=base_path,
                systemd_user=systemd_user or service_name,
                port=port,
                entry_point=entry_point,
                git_branch=git_branch,
                description=description,
                extra_env=extra_env,
                db_type=db_type,
                db_name=db_name,
            )
            msg = f'Сервис «{service_name}» обновлён  <a href="/bots/services/{edit_id}" class="btn btn-sm">Открыть</a>'
        else:
            svc_id = create_service(
                server_name=server,
                project_name=service_name,
                repo_url=repo_url,
                base_path=base_path,
                systemd_user=systemd_user or service_name,
                port=port,
                entry_point=entry_point,
                git_branch=git_branch,
                description=description,
                extra_env=extra_env,
                db_type=db_type,
                db_name=db_name,
            )
            msg = f'Сервис «{service_name}» сохранён (id={svc_id})  <a href="/bots/services" class="btn btn-sm">К списку</a>'
        return f'<div class="toast toast-ok">{msg}</div>'
    except Exception as e:
        return f'<div class="toast toast-fail">Ошибка: {e}</div>'


@app.get("/bots/sqlite-files", response_class=HTMLResponse)
async def sqlite_files(server: str, path: str):
    srv = next((s for s in YAML_CFG.servers if s.name == server), None)
    if not srv:
        return '<span class="muted">Сначала выберите сервер</span>'
    if not path:
        return '<span class="muted">Укажите base path</span>'
    result = _ssh_run_cmd(srv, f"find {shlex.quote(path)} -type f -name '*.db' 2>/dev/null | head -30")
    if not result:
        return '<span class="muted">Не удалось подключиться к серверу</span>'
    out, err, rc = result
    if rc != 0 or not out.strip():
        return '<span class="muted">.db файлы не найдены</span>'
    files = [f.strip() for f in out.strip().split('\n') if f.strip()]
    chips = '<span style="font-size:.85rem;color:var(--muted)">Найдены .db файлы:</span>'
    for f in files:
        chips += f'<code class="sug-chip" onclick="applySqLite(this)" data-value="{f}">{f}</code>'
    return chips


@app.get("/pg-stub", response_class=HTMLResponse)
async def pg_stub(request: Request, server: str, db: str):
    return templates.TemplateResponse(request, "pg_stub.html", {"server": server, "db": db})


@app.get("/bots/scan", response_class=HTMLResponse)
async def bots_scan(request: Request):
    return templates.TemplateResponse(request, "bots.html", {"servers": YAML_CFG.servers})


def _render_svc_cards(services, name, favs):
    cards_html = ""
    for s in services:
        is_fav = s["name"] in favs
        state_cls = {"active": "state-ok", "inactive": "state-warn", "failed": "state-fail"}.get(s["state"], "state-warn")
        fav_btn = (f'<button class="btn btn-sm btn-fav" hx-post="/bots/favorite" '
                   f'hx-vals=\'{{"server_name":"{name}","service_name":"{s["name"]}","op":"remove"}}\' '
                   f'hx-swap="outerHTML" hx-target="this">★</button>') if is_fav else \
                 (f'<button class="btn btn-sm" hx-post="/bots/favorite" '
                  f'hx-vals=\'{{"server_name":"{name}","service_name":"{s["name"]}","op":"add"}}\' '
                  f'hx-swap="outerHTML" hx-target="this">☆</button>')
        pinned_cls = " pinned" if is_fav else ""
        tech_badge = f'<span class="tech-badge">{s["tech"]}</span>' if s["tech"] else ""
        exec_info = ""
        if s["exec_start"]:
            exec_info = f'<div class="bot-exec">{s["user"] + "@" if s["user"] else ""}{s["exec_start"][:120]}{"…" if len(s["exec_start"]) > 120 else ""}</div>'
        action_btns = f'''
  <div class="bot-actions">
    <button class="btn btn-sm" hx-get="/bots/{name}/logs/{s["name"]}" hx-target="#modal-content" hx-trigger="click" onclick="document.getElementById('modal-overlay').style.display='flex'">Лог</button>
    <button class="btn btn-sm" hx-get="/bots/{name}/unit/{s["name"]}" hx-target="#modal-content" hx-trigger="click" onclick="document.getElementById('modal-overlay').style.display='flex'">Файл</button>
  </div>'''
        cards_html += f"""<div class="bot-card{pinned_cls}">
  <div class="bot-card-header">
    <span class="bot-name">{s["name"]}</span>
    {fav_btn}
  </div>
  <div class="bot-desc">{s["desc"]}</div>
  {tech_badge}
  {exec_info}
  <div class="bot-meta">
    <span class="bot-state {state_cls}">{s["state"]}/{s["sub"]}</span>
    <span class="muted">{s["memory"]} RAM</span>
    <span class="muted">{s["uptime"]}</span>
  </div>
  {action_btns}
</div>"""
    return cards_html


_last_scan: dict[str, list[dict]] = {}
_CHUNK = 5


@app.get("/bots/{name}/services", response_class=HTMLResponse)
async def bots_services(name: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'

    result = _ssh_run_cmd(server, r"""
systemctl list-units --type=service --all --no-legend 2>/dev/null | while IFS= read -r line; do
  name=$(echo "$line" | awk '{print $1}')
  [ -z "$name" ] && continue
  echo ">>>SVC:$name"
   systemctl show "$name" --property=Description,ActiveState,SubState,MemoryCurrent,ExecMainStartTimestamp,ExecStart,Type,User,FragmentPath,WorkingDirectory --no-pager 2>/dev/null
  echo "---LOGS:$name"
  journalctl -u "$name" -n 10 --no-pager 2>/dev/null
  echo "<<<END:$name"
done
""")
    if result is None:
        return f'<div class="toast toast-fail">Ошибка подключения к {name}</div>'

    stdout, _, _ = result
    import re
    services = []
    blocks = re.split(r'(?m)^>>>SVC:(\S+)$', stdout)[1:]
    for i in range(0, len(blocks), 2):
        svc_name = blocks[i]
        body = blocks[i + 1]
        parts = re.split(r'(?m)^---LOGS:\S+$', body, maxsplit=1)
        props_text = parts[0] if len(parts) > 0 else ""
        logs_text = parts[1] if len(parts) > 1 else ""
        info = _parse_svc_props(props_text)
        info["name"] = svc_name
        info["logs"] = logs_text.strip()
        info["logs"] = re.sub(r'\n?<<<END:\S+$', '', info["logs"])
        services.append(info)

    if not services:
        return '<div class="bot-none">Сервисы не найдены</div>'

    favs = YAML_CFG.bots.favorites.get(name, [])
    services.sort(key=lambda s: (0 if s["name"] in favs else 1, s["name"]))
    _last_scan[name] = services

    chunk = services[:_CHUNK]
    html = _render_svc_cards(chunk, name, favs)
    if len(services) > _CHUNK:
        return f'<div class="bot-cards" id="bots-{name}-content">\n{html}\n</div>\n<div class="bot-cards" id="bots-{name}-more" hx-get="/bots/{name}/chunk/1" hx-trigger="revealed" hx-swap="outerHTML"><span class="bot-more">↓ загрузить ещё</span></div>'
    return f'<div class="bot-cards" id="bots-{name}-content">\n{html}\n</div>'


@app.get("/bots/{name}/chunk/{chunk_id}", response_class=HTMLResponse)
async def bots_chunk(name: str, chunk_id: int):
    services = _last_scan.get(name, [])
    total = len(services)
    start = chunk_id * _CHUNK
    if start >= total:
        return ""

    chunk = services[start:start + _CHUNK]
    favs = YAML_CFG.bots.favorites.get(name, [])
    html = _render_svc_cards(chunk, name, favs)

    next_id = chunk_id + 1
    if next_id * _CHUNK < total:
        return f'<div class="bot-cards">{html}</div>\n<div class="bot-cards" id="bots-{name}-more" hx-get="/bots/{name}/chunk/{next_id}" hx-trigger="revealed" hx-swap="outerHTML"><span class="bot-more">↓ загрузить ещё</span></div>'
    return f'<div class="bot-cards">{html}</div>'


@app.get("/bots/{name}/logs/{service_name:path}", response_class=HTMLResponse)
async def bots_logs(name: str, service_name: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'
    result = _ssh_run_cmd(server,
        f"journalctl -u '{service_name}' -n 50 --no-pager 2>/dev/null")
    if result is None:
        return '<div class="toast toast-fail">Ошибка подключения</div>'
    out, _, _ = result
    import html as _html
    escaped = _html.escape(out or "(пусто)")
    return f'''<div class="modal-box">
  <span class="modal-close" onclick="document.getElementById('modal-overlay').style.display='none'">&times;</span>
  <div class="modal-header">
    <h3>Лог: {service_name}</h3>
    <div class="modal-header-actions">
      <button class="btn btn-sm" onclick="navigator.clipboard.writeText(document.getElementById('log-text').textContent).then(()=>this.textContent='OK').then(()=>setTimeout(()=>this.textContent='Копировать',1500))">Копировать</button>
      <button class="btn btn-sm" hx-get="/bots/{name}/logs/{service_name}" hx-target="#modal-content" hx-swap="innerHTML">Обновить</button>
    </div>
  </div>
  <pre class="modal-pre" id="log-text">{escaped}</pre>
</div>'''


_PRIO_NAMES = {"emerg": 0, "alert": 1, "crit": 2, "err": 3, "warning": 4, "notice": 5, "info": 6, "debug": 7}

_TEXT_PATTERNS = {
    0: r"\[emerg(?:ency)?\]|(?:^|\s)emerg(?:ency)?\s*:",
    1: r"\[alert\]|(?:^|\s)alert\s*:",
    2: r"\[crit(?:ical)?\]|(?:^|\s)crit(?:ical)?\s*:",
    3: r"\[err(?:or)?\]|(?:^|\s)error\s*:|-\s*error\s*-",
    4: r"\[warn(?:ing)?\]|(?:^|\s)warn(?:ing)?\s*:|-\s*warn(?:ing)?\s*-",
    5: r"\[notice\]|(?:^|\s)notice\s*:",
    6: r"\[info\]|(?:^|\s)info\s*:|-\s*info\s*-",
    7: r"\[debug\]|(?:^|\s)debug\s*:|-\s*debug\s*-",
}


def _level_prio(level: str) -> set:
    return {_PRIO_NAMES[n.strip()] for n in level.split(",") if n.strip() in _PRIO_NAMES} if level else set()


def _level_text_match(msg: str, prio: set) -> bool:
    """Match a selected level present in the message text ([ERROR], ERROR:, - ERROR - ...)."""
    for p in prio:
        pat = _TEXT_PATTERNS.get(p)
        if pat and re.search(pat, msg or "", re.IGNORECASE):
            return True
    return False


def _fmt_shortiso(obj: dict) -> str:
    try:
        ts = int(obj.get("__REALTIME_TIMESTAMP", 0))
        ts_s = datetime.fromtimestamp(ts / 1_000_000).astimezone().isoformat(timespec="seconds")
    except Exception:
        ts_s = ""
    host = obj.get("_HOSTNAME", "")
    ident = obj.get("SYSLOG_IDENTIFIER", obj.get("_COMM", ""))
    pid = obj.get("_PID", "")
    msg = (obj.get("MESSAGE") or "").replace("\n", " ")
    head = f"{ts_s} {host} {ident}"
    return f"{head}[{pid}]: {msg}" if pid else f"{head}: {msg}"


def _scan_journal(text: str, prio: set, limit: int, fetch_n: int) -> str:
    """Parse journalctl -o json output, filter by priority set, return short-iso text."""
    out_lines = []
    for line in text.splitlines():
        try:
            obj = json.loads(line)
        except Exception:
            continue
        p = obj.get("PRIORITY", "")
        try:
            p = int(p)
        except (TypeError, ValueError):
            p = None
        if prio:
            if p not in prio and not _level_text_match(obj.get("MESSAGE", ""), prio):
                continue
        out_lines.append(_fmt_shortiso(obj))
        if len(out_lines) >= limit:
            break
    return "\n".join(out_lines) or "-- No entries --"


@app.get("/api/journal", response_class=HTMLResponse)
async def api_journal(
    server_name: str = "",
    service: str = "",
    lines: int = 100,
    level: str = "",
    since: str = "",
):
    if not server_name:
        return '<div class="muted">Выберите сервер</div>'
    server = next((s for s in YAML_CFG.servers if s.name == server_name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'
    cmd = "journalctl -o json --no-pager"
    if service:
        cmd += f" -u '{service}'"
    if since:
        cmd += f" --since '{since}'"
    prio = _level_prio(level)
    fetch_n = min(lines * 10 if prio else lines, 20000)
    cmd += f" -n {fetch_n}"
    result = _ssh_run_cmd(server, cmd)
    if result is None:
        return '<div class="toast toast-fail">SSH недоступен</div>'
    out, _, rc = result
    import html as _html
    text = _scan_journal(out, prio, lines, fetch_n)
    return f'<pre class="terminal-body log-output term-scroll" id="log-content">{_html.escape(text)}</pre>'


@app.get("/api/journal/stream")
async def api_journal_stream(
    server_name: str = "",
    service: str = "",
    level: str = "",
):
    if not server_name:
        return Response("server_name required", status_code=400)
    server = next((s for s in YAML_CFG.servers if s.name == server_name), None)
    if not server:
        return Response("server not found", status_code=404)

    prio = _level_prio(level)
    cmd_parts = ["journalctl", "-o", "json", "-f", "--no-pager"]
    if service:
        cmd_parts += ["-u", service]

    ssh_cmd = [
        "ssh", "-o", "StrictHostKeyChecking=accept-new",
        "-p", str(server.port),
        f"{server.user}@{server.host}",
        " ".join(cmd_parts)
    ]
    if server.name in _ssh_sessions and _has_sshpass():
        ssh_cmd = ["sshpass", "-p", _ssh_sessions[server.name]] + ssh_cmd

    async def stream():
        proc = await asyncio.create_subprocess_exec(
            *ssh_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            while True:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=30)
                if not line:
                    break
                try:
                    obj = json.loads(line.decode("utf-8", errors="replace"))
                except Exception:
                    continue
                p = obj.get("PRIORITY", "")
                try:
                    p = int(p)
                except (TypeError, ValueError):
                    p = None
                if prio and p not in prio and not _level_text_match(obj.get("MESSAGE", ""), prio):
                    continue
                yield f"data: {_fmt_shortiso(obj)}\n\n"
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            pass
        finally:
            try:
                proc.kill()
            except Exception:
                pass

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/journal/local/stream")
async def api_journal_local_stream(level: str = ""):
    prio = _level_prio(level)
    cmd_parts = ["journalctl", "--user", "-u", "lapg", "-o", "json", "-f", "--no-pager"]

    async def stream():
        proc = await asyncio.create_subprocess_exec(
            *cmd_parts,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            while True:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=30)
                if not line:
                    break
                try:
                    obj = json.loads(line.decode("utf-8", errors="replace"))
                except Exception:
                    continue
                p = obj.get("PRIORITY", "")
                try:
                    p = int(p)
                except (TypeError, ValueError):
                    p = None
                if prio and p not in prio and not _level_text_match(obj.get("MESSAGE", ""), prio):
                    continue
                yield f"data: {_fmt_shortiso(obj)}\n\n"
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            pass
        finally:
            try:
                proc.kill()
            except Exception:
                pass

    return StreamingResponse(stream(), media_type="text/event-stream")


CRON_LOG = PROJECT_DIR / "backup" / "cron.log"


@app.get("/api/cron-log", response_class=HTMLResponse)
async def api_cron_log(lines: int = 200):
    import html as _html
    if not os.path.exists(CRON_LOG):
        return '<pre class="terminal-body">Файл cron.log не найден</pre>'
    try:
        with open(CRON_LOG, "r") as f:
            all_lines = f.readlines()
        tail = all_lines[-min(lines, len(all_lines)):]
        text = "".join(tail)
    except Exception as e:
        return f'<pre class="terminal-body">Ошибка чтения: {_html.escape(str(e))}</pre>'
    return f'<pre class="terminal-body log-output term-scroll" id="log-content">{_html.escape(text or "(пусто)")}</pre>'


@app.get("/api/journal/local", response_class=HTMLResponse)
async def api_journal_local(lines: int = 200, level: str = ""):
    import html as _html
    prio = _level_prio(level)
    fetch_n = min(lines * 10 if prio else lines, 20000)
    cmd = ["journalctl", "--user", "-u", "lapg", "--no-pager", "-o", "json"]
    cmd += ["-n", str(fetch_n)]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        text = _scan_journal(r.stdout, prio, lines, fetch_n)
    except Exception as e:
        return f'<pre class="terminal-body">{_html.escape(str(e))}</pre>'
    return f'<pre class="terminal-body log-output term-scroll" id="log-content">{_html.escape(text)}</pre>'


SELF_UPDATE_LOG = PROJECT_DIR / "tmp" / "self-update.log"


@app.get("/api/self-update-log", response_class=HTMLResponse)
async def api_self_update_log():
    import html as _html
    if not SELF_UPDATE_LOG.exists():
        return '<pre class="terminal-body">Файл tmp/self-update.log не найден</pre>'
    try:
        text = SELF_UPDATE_LOG.read_text()
    except Exception as e:
        return f'<pre class="terminal-body">{_html.escape(str(e))}</pre>'
    return f'<pre class="terminal-body log-output term-scroll" id="log-content">{_html.escape(text or "(пусто)")}</pre>'


@app.get("/api/log-services/{server_name}")
async def api_log_services(server_name: str):
    server = next((s for s in YAML_CFG.servers if s.name == server_name), None)
    if not server:
        return []
    result = _ssh_run_cmd(server,
        "systemctl list-units --type=service --all --no-pager --no-legend 2>/dev/null | awk '{print $1}' | sed 's/\\.service$//'")
    if result is None:
        return []
    out, _, rc = result
    if rc != 0:
        return []
    services = sorted(s.strip() for s in out.strip().splitlines() if s.strip())
    ours = {
        str(s.get("project_name", "")).removesuffix(".service")
        for s in list_services()
        if s.get("server_name") == server_name and s.get("project_name")
    }
    ours |= {str(n).removesuffix(".service") for n in YAML_CFG.bots.favorites.get(server_name, [])}
    return sorted(services, key=lambda x: (x not in ours, x))


@app.get("/api/log-files/{server_name}")
async def api_log_files(server_name: str):
    server = next((s for s in YAML_CFG.servers if s.name == server_name), None)
    if not server:
        return []
    cmd = ("find /root /home /srv /opt /var/log -maxdepth 4 -type f -name '*.log' "
           "2>/dev/null | grep -v '/.npm/\\|/.pm2/\\|/node_modules/\\|/.cache/\\|/.local/' "
           "| sort | head -n 400")
    result = _ssh_run_cmd(server, cmd)
    if result is None:
        return []
    out, _, rc = result
    if rc != 0:
        return []
    return [p.strip() for p in out.strip().splitlines() if p.strip()]


@app.get("/api/ssh-file", response_class=HTMLResponse)
async def api_ssh_file(
    server_name: str = "",
    path: str = "",
    lines: int = 200,
):
    import html as _html
    if not server_name or not path:
        return '<pre class="terminal-body">Не указан файл</pre>'
    if path.startswith("-") or "\n" in path:
        return '<pre class="terminal-body">Некорректный путь</pre>'
    server = next((s for s in YAML_CFG.servers if s.name == server_name), None)
    if not server:
        return '<pre class="terminal-body">Сервер не найден</pre>'
    lines = max(10, min(lines, 5000))
    result = _ssh_run_cmd(server, f"tail -n {lines} -- '{path}'")
    if result is None:
        return '<pre class="terminal-body">SSH недоступен</pre>'
    out, _, rc = result
    if rc != 0:
        return f'<pre class="terminal-body">{_html.escape(out or "Файл не прочитан")}</pre>'
    return f'<pre class="terminal-body log-output term-scroll" id="log-content">{_html.escape(out or "(пусто)")}</pre>'


@app.get("/sources", response_class=HTMLResponse)
async def sources_page(request: Request):
    return templates.TemplateResponse(request, "sources.html", {
        "servers": YAML_CFG.servers,
    })


@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    return templates.TemplateResponse(request, "logs.html", {
        "servers": YAML_CFG.servers,
    })


@app.get("/api/audit-log", response_class=HTMLResponse)
async def api_audit_log(
    action: str = "",
    server: str = "",
    page: int = 1,
):
    per_page = 50
    offset = (page - 1) * per_page
    logs = list_audit_logs(action=action, server=server, limit=per_page, offset=offset)
    total = count_audit_logs(action=action, server=server)
    import html as _html
    if not logs:
        return '<div class="muted">Нет записей</div>'
    rows = ""
    for log_entry in logs:
        ts = log_entry.get("created_at", "")
        act = log_entry.get("action", "")
        srv = log_entry.get("server", "")
        svc = log_entry.get("service", "")
        det = log_entry.get("details", "")
        st = log_entry.get("status", "")
        badge = "badge-green" if st == "ok" else "badge-red"
        rows += f"""<tr>
          <td style="white-space:nowrap">{_html.escape(ts)}</td>
          <td><span class="badge {badge}">{_html.escape(st)}</span></td>
          <td style="font-weight:600">{_html.escape(act)}</td>
          <td>{_html.escape(srv)}</td>
          <td>{_html.escape(svc)}</td>
          <td style="font-size:.8rem;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{_html.escape(det)}">{_html.escape(det[:100])}</td>
        </tr>"""
    pages = max(1, (total + per_page - 1) // per_page)
    pag = ""
    if pages > 1:
        for p in range(1, pages + 1):
            cls = "btn btn-sm btn-primary" if p == page else "btn btn-sm"
            pag += f'<button class="{cls}" onclick="loadAuditPage({p})">{p}</button> '
    return f"""<table class="table" style="width:100%">
  <thead><tr>
    <th>Время</th><th>Статус</th><th>Действие</th><th>Сервер</th><th>Сервис</th><th>Детали</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table>
<div style="display:flex;gap:.3rem;margin-top:.5rem;flex-wrap:wrap">{pag}</div>"""


@app.get("/audit", response_class=HTMLResponse)
async def audit_page(request: Request):
    return templates.TemplateResponse(request, "audit.html", {
        "servers": YAML_CFG.servers,
    })


@app.get("/bots/{name}/unit/{service_name:path}", response_class=HTMLResponse)
async def bots_unit(name: str, service_name: str):
    server = next((s for s in YAML_CFG.servers if s.name == name), None)
    if not server:
        return '<div class="toast toast-fail">Сервер не найден</div>'
    result = _ssh_run_cmd(server,
        f"systemctl cat '{service_name}' 2>/dev/null")
    if result is None:
        return '<div class="toast toast-fail">Ошибка подключения</div>'
    out, _, _ = result
    import html as _html
    escaped = _html.escape(out or "(пусто)")
    return f'''<div class="modal-box">
  <span class="modal-close" onclick="document.getElementById('modal-overlay').style.display='none'">&times;</span>
  <div class="modal-header">
    <h3>Файл: {service_name}</h3>
    <div class="modal-header-actions">
      <button class="btn btn-sm" onclick="navigator.clipboard.writeText(document.getElementById('unit-text').textContent).then(()=>this.textContent='OK').then(()=>setTimeout(()=>this.textContent='Копировать',1500))">Копировать</button>
    </div>
  </div>
  <pre class="modal-pre" id="unit-text">{escaped}</pre>
</div>'''


@app.post("/bots/favorite", response_class=HTMLResponse)
async def bots_favorite(server_name: str = Form(...), service_name: str = Form(...), op: str = Form(...)):
    from settings import save_bot_favorite
    save_bot_favorite(server_name, service_name, op)
    YAML_CFG.bots.favorites.setdefault(server_name, [])
    new_op = "remove" if op == "add" else "add"
    if op == "add":
        if service_name not in YAML_CFG.bots.favorites[server_name]:
            YAML_CFG.bots.favorites[server_name].append(service_name)
    elif op == "remove":
        if service_name in YAML_CFG.bots.favorites[server_name]:
            YAML_CFG.bots.favorites[server_name].remove(service_name)
    star = "★" if new_op == "remove" else "☆"
    cls = "btn-fav" if new_op == "remove" else ""
    return f'<button class="btn btn-sm {cls}" hx-post="/bots/favorite" hx-vals=\'{{"server_name":"{server_name}","service_name":"{service_name}","op":"{new_op}"}}\' hx-swap="outerHTML" hx-target="this">{star}</button>'


def _fmt_size(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
