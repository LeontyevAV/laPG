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

**`.env`** — секреты:
```
DB_PASSWORD=your_password
```

**`settings.yaml`** — всё остальное (создаётся автоматически с дефолтами):
```yaml
connection:
  host: localhost
  user: postgres

backup:
  default_compress: 0
  default_keep: 0
  databases: {}

restore:
  backup_dirs:
    - backup
    - restory

scheduler:
  enabled: false
  cron: "0 2 * * *"
```

## Использование

```bash
# Главное меню
python main.py

# Интерактивный режим (выбрать БД из списка)
python backup.py

# Бекап всех баз данных сразу (параллельно)
python backup.py --all

# Бекап всех БД, хранить 5 последних копий для каждой
python backup.py --all --keep 5

# Максимальное сжатие (уровень 9)
python backup.py -Z 9

# Все опции вместе
python backup.py --all --keep 10 -Z 9

# Восстановить из бекапа
python restore.py
```
