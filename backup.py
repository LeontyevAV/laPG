import asyncio
import argparse
import subprocess
import os
import sys
from datetime import datetime
import asyncpg
from db_utils import fetch_databases, rotate_backups
from settings import get_settings, get_db_backup_config

DB_HOST, DB_USER, DB_PASSWORD, YAML_CFG = get_settings()


def run_pg_dump(db_name, backup_filename, env, compress=0):
    dump_command = [
        "pg_dump",
        "-h", DB_HOST,
        "-U", DB_USER,
        "-d", db_name,
        "-F", "c",
        "-f", backup_filename,
    ]
    if compress:
        dump_command.extend(["-Z", str(compress)])
    try:
        subprocess.run(dump_command, check=True, capture_output=True, text=True, env=env)
        return (True, None)
    except subprocess.CalledProcessError as e:
        return (False, e.stderr)
    except FileNotFoundError:
        return (False, "pg_dump не найден")


def do_backup(db_name, compress=0, keep=0):
    backup_dir = "backup"
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = os.path.join(backup_dir, f"{db_name}_backup_{timestamp}.dump")

    env = os.environ.copy()
    if DB_PASSWORD:
        env["PGPASSWORD"] = DB_PASSWORD

    print(f"{db_name}: {backup_filename}")
    success, error = run_pg_dump(db_name, backup_filename, env, compress)
    if success:
        print("  OK")
        if keep:
            rotate_backups(db_name, keep, backup_dir)
    else:
        print(f"  FAIL: {error}")
    return success


async def backup_all(cli_compress=None, cli_keep=None):
    databases = await fetch_databases(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD
    )
    if not databases:
        print("Нет баз данных для бекапа.")
        return

    def task(db):
        cfg = get_db_backup_config(db, YAML_CFG)
        compress = cli_compress if cli_compress is not None else cfg.compress
        keep = cli_keep if cli_keep is not None else cfg.keep
        return asyncio.to_thread(do_backup, db, compress, keep)

    results = await asyncio.gather(*[task(db) for db in databases])

    ok = sum(1 for r in results if r)
    fail = sum(1 for r in results if not r)
    print(f"\nИтого: {ok} успешно, {fail} с ошибками")
    if fail:
        return False
    return True


def backup_interactive(cli_compress=None, cli_keep=None):
    databases = asyncio.run(fetch_databases(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD
    ))
    if not databases:
        print("На сервере нет доступных баз данных.")
        return False

    print("Доступные базы данных:")
    for i, db_name in enumerate(databases, start=1):
        print(f"{i}. {db_name}")

    while True:
        choice = input("Введите номер базы данных для бекапа: ").strip()
        try:
            index = int(choice) - 1
            if 0 <= index < len(databases):
                break
            print("Ошибка: введите номер из списка.")
        except ValueError:
            print("Ошибка: введите число.")

    db_name = databases[index]
    cfg = get_db_backup_config(db_name, YAML_CFG)
    compress = cli_compress if cli_compress is not None else cfg.compress
    keep = cli_keep if cli_keep is not None else cfg.keep
    return do_backup(db_name, compress, keep)


def cli():
    parser = argparse.ArgumentParser(description="Бекап PostgreSQL баз данных")
    parser.add_argument("--all", action="store_true", help="бекапить все базы данных")
    parser.add_argument("--keep", type=int, default=None, help="хранить N последних бекапов для каждой БД (0 = все)")
    parser.add_argument("-Z", "--compress", type=int, default=None, choices=range(0, 10), help="уровень сжатия pg_dump 0-9")
    args = parser.parse_args()

    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        print("Ошибка: проверьте DB_PASSWORD в .env и настройки connection в settings.yaml")
        sys.exit(1)

    if args.all:
        ok = asyncio.run(backup_all(args.compress, args.keep))
    else:
        ok = backup_interactive(args.compress, args.keep)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    cli()
