import asyncio
import subprocess
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import asyncpg

# Загружаем переменные из .env файла
load_dotenv()

# Получаем параметры подключения из переменных окружения
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_USER, DB_PASSWORD]):
    raise ValueError(
        "Не все необходимые переменные окружения установлены в .env файле."
    )


async def fetch_databases():
    conn = await asyncpg.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database="postgres",
    )
    rows = await conn.fetch(
        "SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false ORDER BY datname"
    )
    await conn.close()
    return [row["datname"] for row in rows]


try:
    databases = asyncio.run(fetch_databases())
except asyncpg.PostgresError as e:
    print(f"Ошибка подключения к серверу: {e}")
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

# Установка переменной окружения для пароля (чтобы pg_dump не запрашивал её интерактивно)
env = os.environ.copy()
env["PGPASSWORD"] = DB_PASSWORD

# Создаем папку backup, если она не существует
backup_dir = "backup"
os.makedirs(backup_dir, exist_ok=True)

# Генерируем имя файла бэкапа с меткой времени и помещаем его в папку backup
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup_filename = os.path.join(backup_dir, f"{DB_NAME}_backup_{timestamp}.dump")

# Команда pg_dump
dump_command = [
    "pg_dump",
    "-h",
    DB_HOST,
    "-U",
    DB_USER,
    "-d",
    DB_NAME,
    "-F",
    "c",  # Формат копии (custom), позволяет гибко восстанавливать
    "-f",
    backup_filename,
]

try:
    print(f"Начинаю создание backup... Файл: {backup_filename}")
    result = subprocess.run(
        dump_command, check=True, capture_output=True, text=True, env=env
    )
    print("Backup успешно создан.")
    # При необходимости можно вывести логи из result.stdout
except subprocess.CalledProcessError as e:
    print(f"Ошибка при создании backup: {e.stderr}")
except FileNotFoundError:
    print(
        "Ошибка: команда 'pg_dump' не найдена. Убедитесь, что утилиты PostgreSQL установлены и находятся в PATH."
    )
