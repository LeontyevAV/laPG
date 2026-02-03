import subprocess
import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# --- Учетные данные для подключения ---
# Для создания БД часто нужен суперпользователь или пользователь с правами CREATEDB
DB_HOST = os.getenv("DB_HOST")
DB_SUPERUSER = os.getenv("DB_SUPERUSER")  # Пользователь с правами CREATEDB
DB_SUPERUSER_PASSWORD = os.getenv("DB_SUPERUSER_PASSWORD")

# Имя новой базы данных, которую нужно создать и в которую восстановить
NEW_DB_NAME = input(
    "Введите имя новой базы данных для создания и восстановления: "
).strip()

if not NEW_DB_NAME or not DB_HOST or not DB_SUPERUSER or not DB_SUPERUSER_PASSWORD:
    raise ValueError(
        "Не все необходимые переменные окружения установлены в .env файле или имя БД не введено."
    )

# Путь к папке с бэкапами
BACKUP_DIR = "backup"

# --- Список доступных бэкапов ---
backup_files = [f for f in os.listdir(BACKUP_DIR) if f.endswith(".dump")]
if not backup_files:
    print(f"В папке '{BACKUP_DIR}' не найдено файлов бэкапа (.dump).")
    exit(1)

print("Доступные файлы бэкапа:")
for i, f in enumerate(backup_files):
    print(f"  {i + 1}. {f}")

# --- Выбор файла бэкапа ---
try:
    choice_num = int(input("Выберите номер файла бэкапа для восстановления: ")) - 1
    if choice_num < 0 or choice_num >= len(backup_files):
        raise ValueError("Неверный номер выбора.")
except (ValueError, TypeError):
    print("Неверный ввод. Введите число.")
    exit(1)

selected_backup_file = os.path.join(BACKUP_DIR, backup_files[choice_num])
print(f"Выбран файл: {selected_backup_file}")

# --- Установка переменной окружения для пароля ---
env = os.environ.copy()
env["PGPASSWORD"] = DB_SUPERUSER_PASSWORD

# --- Проверка существования базы данных ---
print(f"\n--- Проверка существования базы данных '{NEW_DB_NAME}' ---")
# SQL-запрос для проверки
check_sql = f"SELECT 1 FROM pg_database WHERE datname = '{NEW_DB_NAME}';"
check_db_command = [
    "psql",
    "-h",
    DB_HOST,
    "-U",
    DB_SUPERUSER,
    "-d",
    "postgres",  # Подключаемся к служебной БД 'postgres'
    "-t",  # тихий режим (только результаты)
    "-c",
    check_sql,  # выполнить команду
]

try:
    result_check = subprocess.run(
        check_db_command, check=True, capture_output=True, text=True, env=env
    )
    # Если запрос вернул '1', значит база существует
    if "1" in result_check.stdout:
        print(f"Внимание: База данных '{NEW_DB_NAME}' уже существует.")
        while True:
            overwrite_choice = input("Перезаписать её? (yes/no): ").lower()
            if overwrite_choice in ["yes", "y"]:
                print(
                    f"Продолжаем. Существующая база '{NEW_DB_NAME}' будет перезаписана."
                )
                # ШАГ 1: Удалить существующую базу
                print(f"--- Удаление существующей базы данных '{NEW_DB_NAME}' ---")
                drop_db_command = [
                    "dropdb",
                    "-h",
                    DB_HOST,
                    "-U",
                    DB_SUPERUSER,
                    NEW_DB_NAME,
                ]
                try:
                    result_drop = subprocess.run(
                        drop_db_command,
                        check=True,
                        capture_output=True,
                        text=True,
                        env=env,
                    )
                    print(f"База данных '{NEW_DB_NAME}' успешно удалена.")
                except subprocess.CalledProcessError as e:
                    print(
                        f"Ошибка при удалении базы данных '{NEW_DB_NAME}': {e.stderr}"
                    )
                    exit(1)
                break  # выйти из цикла while после успешного удаления
            elif overwrite_choice in ["no", "n"]:
                print("Операция отменена пользователем.")
                exit(0)
            else:
                print("Пожалуйста, введите 'yes' или 'no'.")
    else:
        print(f"База данных '{NEW_DB_NAME}' не найдена. Продолжаем создание...")
except subprocess.CalledProcessError as e:
    print(f"Ошибка при проверке существования базы данных: {e.stderr}")
    exit(1)


# --- 1. Создание новой базы данных (или повторное создание после удаления)---
print(f"\n--- Создание базы данных '{NEW_DB_NAME}' ---")
create_db_command = [
    "createdb",
    "-h",
    DB_HOST,
    "-U",
    DB_SUPERUSER,
    # '-T', 'template0', # Можно указать шаблон, если нужно чистое состояние
    NEW_DB_NAME,
]

try:
    result_create = subprocess.run(
        create_db_command, check=True, capture_output=True, text=True, env=env
    )
    print(f"База данных '{NEW_DB_NAME}' успешно создана.")
except subprocess.CalledProcessError as e:
    print(f"Ошибка при создании базы данных '{NEW_DB_NAME}': {e.stderr}")
    # Важно: если база не создалась, дальнейшие действия бессмысленны
    exit(1)

# --- 2. Восстановление бэкапа в новую базу ---
print(
    f"\n--- Восстановление бэкапа из '{selected_backup_file}' в базу '{NEW_DB_NAME}' ---"
)
restore_command = [
    "pg_restore",
    "-h",
    DB_HOST,
    "-U",
    DB_SUPERUSER,
    "-d",
    NEW_DB_NAME,  # Указываем имя новой БД
    "-v",  # verbose для подробного лога (опционально)
    selected_backup_file,
]

try:
    result_restore = subprocess.run(
        restore_command, check=True, capture_output=True, text=True, env=env
    )
    print(f"Восстановление в базу данных '{NEW_DB_NAME}' успешно завершено.")
    # При желании можно вывести result_restore.stdout или stderr для отладки
except subprocess.CalledProcessError as e:
    print(f"Ошибка при восстановлении бэкапа в '{NEW_DB_NAME}': {e.stderr}")
    # В идеале, если восстановление не удалось, можно удалить созданную базу
    # drop_db_command = ['dropdb', '-h', DB_HOST, '-U', DB_SUPERUSER, NEW_DB_NAME]
    # subprocess.run(drop_db_command, capture_output=True, env=env) # Попробовать удалить
    exit(1)

print("\n--- Процесс восстановления завершен ---")
