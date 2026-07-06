import asyncio
import socket
import subprocess
import os
import sys
import re
import time
import asyncpg
from db_utils import fetch_databases
from settings import get_settings, save_remote_config

DB_HOST, DB_SUPERUSER, DB_SUPERUSER_PASSWORD, YAML_CFG = get_settings()

if not all([DB_HOST, DB_SUPERUSER, DB_SUPERUSER_PASSWORD]):
    raise ValueError(
        "Не все необходимые переменные окружения установлены в .env файле."
    )

env = os.environ.copy()
env["PGPASSWORD"] = DB_SUPERUSER_PASSWORD

_ssh_proc = None


def _free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _start_ssh_tunnel():
    rc = YAML_CFG.remote

    print("\nПодключение через SSH-туннель")
    default_host = rc.host or ""
    default_ssh_port = str(rc.port) if rc.port else "22"
    default_pg_port = str(rc.pg_port) if rc.pg_port else "5432"
    default_user = rc.user or "root"

    prompt_host = f"  Хост [{default_host}]: " if default_host else "  Хост: "
    ssh_host = input(prompt_host).strip() or default_host
    if not ssh_host:
        print("  Хост не указан.")
        return None
    ssh_port = input(f"  SSH порт [{default_ssh_port}]: ").strip() or default_ssh_port
    pg_port = input(f"  Порт PostgreSQL [{default_pg_port}]: ").strip() or default_pg_port
    ssh_user = input(f"  Пользователь [{default_user}]: ").strip() or default_user

    local_port = _free_port()
    cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=accept-new",
        "-L", f"{local_port}:localhost:{pg_port}",
        "-p", ssh_port,
        "-N",
        f"{ssh_user}@{ssh_host}",
    ]

    global _ssh_proc
    print(f"  Запуск туннеля localhost:{local_port} → {ssh_host}:{pg_port}...")
    try:
        _ssh_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1)
        if _ssh_proc.poll() is not None:
            print(f"  Ошибка: туннель не установлен (код {_ssh_proc.returncode})")
            _ssh_proc = None
            return None
        print("  ✓ Туннель установлен")
        save_remote_config(ssh_host, ssh_user, int(ssh_port), int(pg_port))

        import getpass
        pg_pass = getpass.getpass("  Пароль PostgreSQL: ")
        return local_port, pg_pass
    except FileNotFoundError:
        print("  Ошибка: ssh не найден")
        return None


def _stop_ssh_tunnel():
    global _ssh_proc
    if _ssh_proc:
        _ssh_proc.terminate()
        try:
            _ssh_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _ssh_proc.kill()
        _ssh_proc = None
        print("  Туннель закрыт")
        env["PGPASSWORD"] = DB_SUPERUSER_PASSWORD


def extract_db_name_from_dump(dump_path):
    try:
        result = subprocess.run(
            ["pg_restore", "-l", dump_path],
            capture_output=True, text=True,
        )
        for line in result.stdout.splitlines():
            m = re.search(r"dbname:\s*(\S+)", line, re.IGNORECASE)
            if m:
                return m.group(1)
    except FileNotFoundError:
        pass
    return None


def extract_db_name_from_filename(filepath):
    basename = os.path.basename(filepath)
    m = re.match(r"^(.+)_backup_\d{8}_\d{6}\.dump$", basename)
    if m:
        return m.group(1)
    return None


def list_dump_files():
    entries = []
    for d in YAML_CFG.restore.backup_dirs:
        if not os.path.isdir(d):
            continue
        for f in os.listdir(d):
            if f.endswith(".dump"):
                entries.append(os.path.join(d, f))
    return sorted(entries)


def pick_dump(entries):
    print("Доступные файлы бэкапа:")
    for i, entry in enumerate(entries, start=1):
        print(f"{i}. {entry}")
    while True:
        choice = input("Выберите номер файла: ").strip()
        try:
            index = int(choice) - 1
            if 0 <= index < len(entries):
                print(f"Выбран файл: {entries[index]}\n")
                return entries[index]
            print("Ошибка: введите номер из списка.")
        except ValueError:
            print("Ошибка: введите число.")


def run_psql(db_action, target_db, tunnel_port=None):
    host = "localhost" if tunnel_port else DB_HOST
    cmd = [db_action, "-h", host, "-U", DB_SUPERUSER]
    if tunnel_port:
        cmd.extend(["-p", str(tunnel_port)])
    if db_action in ("dropdb", "createdb"):
        cmd.append(target_db)
    else:
        cmd.extend(["-d", "postgres", "-t", "-c", target_db])
    return subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)


def restore_dump(dump_path, target_db, tunnel_port=None):
    print(f"Восстановление из '{dump_path}' в '{target_db}'...")
    host = "localhost" if tunnel_port else DB_HOST
    cmd = [
        "pg_restore",
        "-h", host,
        "-U", DB_SUPERUSER,
        "-d", target_db,
        dump_path,
    ]
    if tunnel_port:
        cmd.extend(["-p", str(tunnel_port)])
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
        print("Восстановление успешно завершено.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при восстановлении: {e.stderr}")


def restore_interactive():
    # SSH tunnel
    tunnel_port = None
    remote_pg_pass = None
    dest = input("Восстановить на локальном (l) или удалённом (r) сервере? [l]: ").strip().lower()
    if dest in ("r", "remote"):
        tunnel_result = _start_ssh_tunnel()
        if tunnel_result is None:
            return
        tunnel_port, remote_pg_pass = tunnel_result
        env["PGPASSWORD"] = remote_pg_pass

    try:
        databases = asyncio.run(fetch_databases(
            host="localhost" if tunnel_port else DB_HOST,
            user=DB_SUPERUSER, password=remote_pg_pass if remote_pg_pass else DB_SUPERUSER_PASSWORD,
            port=tunnel_port or 5432,
        ))
    except (ConnectionError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
        print(f"Ошибка подключения: {e}")
        _stop_ssh_tunnel()
        return

    print("0. Создать новую БД")
    for i, db_name in enumerate(databases, start=1):
        print(f"{i}. {db_name}")

    while True:
        choice = input("Выберите номер целевой БД (или 0 для новой): ").strip()
        try:
            num = int(choice)
            if num == 0:
                target_db = None
                break
            if 1 <= num <= len(databases):
                target_db = databases[num - 1]
                break
            print("Ошибка: введите номер из списка.")
        except ValueError:
            print("Ошибка: введите число.")

    dump_files = list_dump_files()
    if not dump_files:
        print("Не найдено файлов .dump в папках backup/ или restore/.")
        _stop_ssh_tunnel()
        return

    if target_db is None:
        dump_path = pick_dump(dump_files)

        db_name = extract_db_name_from_dump(dump_path)
        if not db_name:
            db_name = extract_db_name_from_filename(dump_path)

        if db_name:
            prompt = f"Имя базы данных для восстановления [{db_name}] или введи другое: "
            inp = input(prompt).strip()
            target_db = inp if inp else db_name
        else:
            target_db = input("Введите имя базы данных: ").strip()

        if not target_db:
            print("Имя не может быть пустым.")
            _stop_ssh_tunnel()
            return

        check_sql = f"SELECT 1 FROM pg_database WHERE datname = '{target_db}'"
        try:
            result = run_psql("psql", check_sql, tunnel_port)
            db_exists = "1" in result.stdout
        except subprocess.CalledProcessError as e:
            print(f"Ошибка при проверке БД: {e.stderr}")
            _stop_ssh_tunnel()
            return

        if db_exists:
            print(f"База '{target_db}' уже существует.")
            answer = input("Перезаписать? (yes/no): ").strip().lower()
            if answer not in ("yes", "y"):
                print("Операция отменена.")
                _stop_ssh_tunnel()
                return
            print(f"Удаление базы '{target_db}'...")
            try:
                run_psql("dropdb", target_db, tunnel_port)
                print("База удалена.")
            except subprocess.CalledProcessError as e:
                print(f"Ошибка при удалении: {e.stderr}")
                _stop_ssh_tunnel()
                return
    else:
        print(f"Целевая БД: {target_db}")
        dump_path = pick_dump(dump_files)

        print("База будет перезаписана.")
        answer = input("Продолжить? (yes/no): ").strip().lower()
        if answer not in ("yes", "y"):
            print("Операция отменена.")
            _stop_ssh_tunnel()
            return

        print(f"Удаление базы '{target_db}'...")
        try:
            run_psql("dropdb", target_db, tunnel_port)
            print("База удалена.")
        except subprocess.CalledProcessError as e:
            print(f"Ошибка при удалении: {e.stderr}")
            _stop_ssh_tunnel()
            return

    print(f"Создание базы '{target_db}'...")
    try:
        run_psql("createdb", target_db, tunnel_port)
        print("База создана.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при создании: {e.stderr}")
        _stop_ssh_tunnel()
        return

    restore_dump(dump_path, target_db, tunnel_port)
    _stop_ssh_tunnel()


if __name__ == "__main__":
    restore_interactive()
