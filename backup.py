import asyncio
import subprocess
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import asyncpg
from db_utils import fetch_databases

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_USER, DB_PASSWORD]):
    raise ValueError(
        "Не все необходимые переменные окружения установлены в .env файле."
    )

try:
    databases = asyncio.run(fetch_databases(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD
    ))
except (ConnectionError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
    print(f"Ошибка: {e}")
    sys.exit(1)

if not databases:
    print("На сервере нет доступных баз данных.")
    sys.exit(1)

print("Доступные базы данных:")
for i, db_name in enumerate(databases, start=1):
    print(f"{i}. {db_name}")

DB_NAME = None
while DB_NAME is None:
    choice = input("Введите номер базы данных для бекапа: ").strip()
    try:
        index = int(choice) - 1
        if index < 0 or index >= len(databases):
            print("Ошибка: введите номер из списка.")
            continue
        DB_NAME = databases[index]
    except ValueError:
        print("Ошибка: введите число.")

env = os.environ.copy()
env["PGPASSWORD"] = DB_PASSWORD

backup_dir = "backup"
os.makedirs(backup_dir, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup_filename = os.path.join(backup_dir, f"{DB_NAME}_backup_{timestamp}.dump")

dump_command = [
    "pg_dump",
    "-h", DB_HOST,
    "-U", DB_USER,
    "-d", DB_NAME,
    "-F", "c",
    "-f", backup_filename,
]

try:
    print(f"Начинаю создание backup... Файл: {backup_filename}")
    result = subprocess.run(
        dump_command, check=True, capture_output=True, text=True, env=env
    )
    print("Backup успешно создан.")
except subprocess.CalledProcessError as e:
    print(f"Ошибка при создании backup: {e.stderr}")
    sys.exit(1)
except FileNotFoundError:
    print(
        "Ошибка: команда 'pg_dump' не найдена. "
        "Убедитесь, что утилиты PostgreSQL установлены и находятся в PATH."
    )
    sys.exit(1)
