import asyncio
import subprocess
import os
import sys
import asyncpg
from dotenv import load_dotenv
from db_utils import fetch_databases

load_dotenv()

DB_HOST = os.getenv("DB_HOST") or "localhost"
DB_SUPERUSER = os.getenv("DB_USER") or "postgres"
DB_SUPERUSER_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_SUPERUSER, DB_SUPERUSER_PASSWORD]):
    raise ValueError(
        "Не все необходимые переменные окружения установлены в .env файле."
    )

# --- Выбор целевой БД ---
try:
    databases = asyncio.run(fetch_databases(
        host=DB_HOST, user=DB_SUPERUSER, password=DB_SUPERUSER_PASSWORD
    ))
except (ConnectionError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
    print(f"Ошибка подключения: {e}")
    sys.exit(1)

print("Целевые базы данных на сервере (можно перезаписать):")
for i, db_name in enumerate(databases, start=1):
    print(f"{i}. {db_name}")
print("0. Ввести новое имя")

NEW_DB_NAME = None
while NEW_DB_NAME is None:
    choice = input("Выберите номер или введите 0 для нового имени: ").strip()
    try:
        num = int(choice)
        if num == 0:
            custom = input("Введите имя новой базы данных: ").strip()
            if not custom:
                print("Имя не может быть пустым.")
                continue
            NEW_DB_NAME = custom
        elif 1 <= num <= len(databases):
            NEW_DB_NAME = databases[num - 1]
        else:
            print("Ошибка: введите номер из списка.")
    except ValueError:
        print("Ошибка: введите число.")

# --- Выбор файла бэкапа ---
BACKUP_DIR = "backup"
try:
    all_files = os.listdir(BACKUP_DIR)
except FileNotFoundError:
    print(f"Папка '{BACKUP_DIR}' не найдена.")
    sys.exit(1)

backup_files = sorted([f for f in all_files if f.endswith(".dump")])
if not backup_files:
    print(f"В папке '{BACKUP_DIR}' не найдено файлов .dump.")
    sys.exit(1)

print("\nДоступные файлы бэкапа:")
for i, f in enumerate(backup_files, start=1):
    print(f"{i}. {f}")

selected_backup = None
while selected_backup is None:
    choice = input("Выберите номер файла для восстановления: ").strip()
    try:
        index = int(choice) - 1
        if index < 0 or index >= len(backup_files):
            print("Ошибка: введите номер из списка.")
            continue
        selected_backup = os.path.join(BACKUP_DIR, backup_files[index])
    except ValueError:
        print("Ошибка: введите число.")

print(f"Выбран файл: {selected_backup}\n")

# --- Подготовка окружения ---
env = os.environ.copy()
env["PGPASSWORD"] = DB_SUPERUSER_PASSWORD

# --- Проверка существования БД ---
check_sql = f"SELECT 1 FROM pg_database WHERE datname = '{NEW_DB_NAME}'"
check_cmd = [
    "psql",
    "-h", DB_HOST,
    "-U", DB_SUPERUSER,
    "-d", "postgres",
    "-t",
    "-c", check_sql,
]

try:
    result = subprocess.run(check_cmd, check=True, capture_output=True, text=True, env=env)
    db_exists = "1" in result.stdout
except subprocess.CalledProcessError as e:
    print(f"Ошибка при проверке БД: {e.stderr}")
    sys.exit(1)

if db_exists:
    print(f"База данных '{NEW_DB_NAME}' уже существует.")
    answer = input("Перезаписать? (yes/no): ").strip().lower()
    if answer not in ("yes", "y"):
        print("Операция отменена.")
        sys.exit(0)
    print(f"Удаление базы '{NEW_DB_NAME}'...")
    drop_cmd = ["dropdb", "-h", DB_HOST, "-U", DB_SUPERUSER, NEW_DB_NAME]
    try:
        subprocess.run(drop_cmd, check=True, capture_output=True, text=True, env=env)
        print("База удалена.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при удалении базы: {e.stderr}")
        sys.exit(1)

# --- Создание БД ---
print(f"Создание базы '{NEW_DB_NAME}'...")
create_cmd = ["createdb", "-h", DB_HOST, "-U", DB_SUPERUSER, NEW_DB_NAME]
try:
    subprocess.run(create_cmd, check=True, capture_output=True, text=True, env=env)
    print("База создана.")
except subprocess.CalledProcessError as e:
    print(f"Ошибка при создании базы: {e.stderr}")
    sys.exit(1)

# --- Восстановление ---
print(f"Восстановление из '{selected_backup}' в '{NEW_DB_NAME}'...")
restore_cmd = [
    "pg_restore",
    "-h", DB_HOST,
    "-U", DB_SUPERUSER,
    "-d", NEW_DB_NAME,
    selected_backup,
]
try:
    subprocess.run(restore_cmd, check=True, capture_output=True, text=True, env=env)
    print("Восстановление успешно завершено.")
except subprocess.CalledProcessError as e:
    print(f"Ошибка при восстановлении: {e.stderr}")
    sys.exit(1)
