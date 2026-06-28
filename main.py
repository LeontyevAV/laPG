import asyncio
import os
import sys
import subprocess
from datetime import datetime
import asyncpg
from db_utils import fetch_databases
from backup import backup_interactive, backup_all
from restore import (
    restore_interactive, list_dump_files, extract_db_name_from_dump,
    extract_db_name_from_filename, pick_dump,
)
from settings import get_settings
from cron import cron_manager

DB_HOST, DB_USER, DB_PASSWORD, YAML_CFG = get_settings()


def show_backups():
    entries = list_dump_files()
    if not entries:
        dirs = "/".join(YAML_CFG.restore.backup_dirs)
        print(f"Нет файлов .dump в {dirs}/.")
        return
    print(f"{'Файл':<55} {'Размер':>10}  {'Дата'}")
    print("-" * 80)
    for e in entries:
        size = os.path.getsize(e)
        mtime = datetime.fromtimestamp(os.path.getmtime(e)).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{e:<55} {size:>10}  {mtime}")


def show_databases():
    try:
        dbs = asyncio.run(fetch_databases(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD
        ))
    except (ConnectionError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
        print(f"Ошибка: {e}")
        return
    print("Базы данных на сервере:")
    for i, db in enumerate(dbs, 1):
        print(f"  {i}. {db}")


def delete_database():
    try:
        dbs = asyncio.run(fetch_databases(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD
        ))
    except (ConnectionError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
        print(f"Ошибка: {e}")
        return

    for i, db in enumerate(dbs, 1):
        print(f"  {i}. {db}")
    while True:
        choice = input("Введите номер БД для удаления: ").strip()
        try:
            index = int(choice) - 1
            if 0 <= index < len(dbs):
                break
            print("Ошибка: введите номер из списка.")
        except ValueError:
            print("Ошибка: введите число.")

    target = dbs[index]
    answer = input(f"Удалить базу '{target}'? (yes/no): ").strip().lower()
    if answer not in ("yes", "y"):
        print("Отменено.")
        return

    env = os.environ.copy()
    if DB_PASSWORD:
        env["PGPASSWORD"] = DB_PASSWORD

    try:
        subprocess.run(
            ["dropdb", "-h", DB_HOST, "-U", DB_USER, target],
            check=True, capture_output=True, text=True, env=env,
        )
        print(f"База '{target}' удалена.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка: {e.stderr}")


def dump_info():
    entries = list_dump_files()
    if not entries:
        print("Нет файлов .dump в backup/ или restore/.")
        return

    dump_path = pick_dump(entries)
    size = os.path.getsize(dump_path)
    mtime = datetime.fromtimestamp(os.path.getmtime(dump_path))
    db_from_dump = extract_db_name_from_dump(dump_path)
    db_from_file = extract_db_name_from_filename(dump_path)

    print(f"\nФайл:     {dump_path}")
    print(f"Размер:   {size} bytes")
    print(f"Изменён:  {mtime.strftime('%Y-%m-%d %H:%M:%S')}")
    if db_from_dump:
        print(f"БД (из дампа): {db_from_dump}")
    if db_from_file:
        print(f"БД (из имени): {db_from_file}")


def main():
    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        print("Ошибка: проверьте connection в settings.yaml и DB_PASSWORD в .env")
        sys.exit(1)

    while True:
        print("\n--- laPG ---")
        print("1. Бекап БД")
        print("2. Бекап всех БД")
        print("3. Восстановить БД")
        print("4. Удалить БД")
        print("5. Список бекапов")
        print("6. Список БД на сервере")
        print("7. Инфо о дампе")
        print("8. Настройка cron")
        print("0. Выход")

        choice = input("\nВыберите пункт: ").strip()
        print()

        if choice == "1":
            backup_interactive()
        elif choice == "2":
            asyncio.run(backup_all())
        elif choice == "3":
            restore_interactive()
        elif choice == "4":
            delete_database()
        elif choice == "5":
            show_backups()
        elif choice == "6":
            show_databases()
        elif choice == "7":
            dump_info()
        elif choice == "8":
            cron_manager()
        elif choice == "0":
            print("До свидания.")
            break
        else:
            print("Неверный пункт. Введите 0-8.")

        if choice in ("1", "2", "3", "4"):
            input("\nНажмите Enter для продолжения...")


if __name__ == "__main__":
    main()
