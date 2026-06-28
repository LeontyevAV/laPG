# laPG

CLI-утилита для резервного копирования и восстановления PostgreSQL баз данных.

## Установка

```bash
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

## Настройка

Создайте `.env` в корне проекта:

```
DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=your_password
```

## Использование

```bash
# Создать бекап (выбрать БД из списка на сервере)
python backup.py

# Восстановить из бекапа
python restore.py
```
