import subprocess
import os
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Получаем параметры подключения из переменных окружения
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Имя новой базы данных, которую нужно создать и в которую восстановить
DB_NAME = input("Введите имя новой базы данных для создания и восстановления: ").strip()

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError(
        "Не все необходимые переменные окружения установлены в .env файле."
    )

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
