import asyncio
import subprocess
import os
import sys
import re
import asyncpg
from db_utils import fetch_databases
from settings import get_settings

DB_HOST, DB_SUPERUSER, DB_SUPERUSER_PASSWORD, YAML_CFG = get_settings()

if not all([DB_HOST, DB_SUPERUSER, DB_SUPERUSER_PASSWORD]):
    raise ValueError(
        "Не все необходимые переменные окружения установлены в .env файле."
    )

env = os.environ.copy()
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


def run_psql(db_action, target_db):
    cmd = [db_action, "-h", DB_HOST, "-U", DB_SUPERUSER]
    if db_action in ("dropdb", "createdb"):
        cmd.append(target_db)
    else:
        cmd.extend(["-d", "postgres", "-t", "-c", target_db])
    return subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)


def restore_dump(dump_path, target_db):
    print(f"Восстановление из '{dump_path}' в '{target_db}'...")
    cmd = [
        "pg_restore",
        "-h", DB_HOST,
        "-U", DB_SUPERUSER,
        "-d", target_db,
        dump_path,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
        print("Восстановление успешно завершено.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при восстановлении: {e.stderr}")


def restore_interactive():
    try:
        databases = asyncio.run(fetch_databases(
            host=DB_HOST, user=DB_SUPERUSER, password=DB_SUPERUSER_PASSWORD
        ))
    except (ConnectionError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
        print(f"Ошибка подключения: {e}")
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
            return

        check_sql = f"SELECT 1 FROM pg_database WHERE datname = '{target_db}'"
        try:
            result = run_psql("psql", check_sql)
            db_exists = "1" in result.stdout
        except subprocess.CalledProcessError as e:
            print(f"Ошибка при проверке БД: {e.stderr}")
            return

        if db_exists:
            print(f"База '{target_db}' уже существует.")
            answer = input("Перезаписать? (yes/no): ").strip().lower()
            if answer not in ("yes", "y"):
                print("Операция отменена.")
                return
            print(f"Удаление базы '{target_db}'...")
            try:
                run_psql("dropdb", target_db)
                print("База удалена.")
            except subprocess.CalledProcessError as e:
                print(f"Ошибка при удалении: {e.stderr}")
                return
    else:
        print(f"Целевая БД: {target_db}")
        dump_path = pick_dump(dump_files)

        print("База будет перезаписана.")
        answer = input("Продолжить? (yes/no): ").strip().lower()
        if answer not in ("yes", "y"):
            print("Операция отменена.")
            return

        print(f"Удаление базы '{target_db}'...")
        try:
            run_psql("dropdb", target_db)
            print("База удалена.")
        except subprocess.CalledProcessError as e:
            print(f"Ошибка при удалении: {e.stderr}")
            return

    print(f"Создание базы '{target_db}'...")
    try:
        run_psql("createdb", target_db)
        print("База создана.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при создании: {e.stderr}")
        return

    restore_dump(dump_path, target_db)


if __name__ == "__main__":
    restore_interactive()
