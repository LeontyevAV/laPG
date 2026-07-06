import asyncio
import re
import asyncpg
from db_utils import fetch_databases
from settings import get_settings

_NEXTVAL_RE = re.compile(r"nextval\('[^']*'::regclass\)")

DB_HOST, DB_USER, DB_PASSWORD, YAML_CFG = get_settings()


def _normalize_default(val):
    if val is None:
        return None
    return _NEXTVAL_RE.sub(":nextval", val)


def _normalize_schema(schema):
    for tbl in schema:
        for col in schema[tbl]:
            schema[tbl][col]["default"] = _normalize_default(schema[tbl][col]["default"])


def _pick_db(prompt, dbs):
    while True:
        choice = input(prompt).strip()
        try:
            index = int(choice) - 1
            if 0 <= index < len(dbs):
                return dbs[index]
            print("Ошибка: введите номер из списка.")
        except ValueError:
            print("Ошибка: введите число.")


async def _fetch_schema(host, user, password, db_name):
    conn = await asyncpg.connect(
        host=host, user=user, password=password, database=db_name, timeout=5
    )
    rows = await conn.fetch(
        """
        SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
        """
    )
    await conn.close()
    schema = {}
    for r in rows:
        tbl = r["table_name"]
        if tbl not in schema:
            schema[tbl] = {}
        schema[tbl][r["column_name"]] = {
            "type": r["data_type"],
            "nullable": r["is_nullable"] == "YES",
            "default": r["column_default"],
        }
    return schema


def _compare_tables_list(name_a, schema_a, name_b, schema_b):
    tables_a = set(schema_a)
    tables_b = set(schema_b)

    only_a = tables_a - tables_b
    only_b = tables_b - tables_a
    common = tables_a & tables_b

    print(f"\n  Всего таблиц: '{name_a}' — {len(tables_a)}, '{name_b}' — {len(tables_b)}")
    print(f"  Совпадает: {len(common)}")

    if common:
        for t in sorted(common):
            print(f"    ✓ {t}")

    if only_a:
        print(f"\n  Таблицы только в '{name_a}':")
        for t in sorted(only_a):
            print(f"    - {t} ({len(schema_a[t])} колонок)")

    if only_b:
        print(f"\n  Таблицы только в '{name_b}':")
        for t in sorted(only_b):
            print(f"    - {t} ({len(schema_b[t])} колонок)")


def _compare_common_tables(name_a, schema_a, name_b, schema_b):
    common = set(schema_a) & set(schema_b)

    if not common:
        print("\n  Нет общих таблиц.")
        return

    print(f"\n  Одноимённые таблицы ({len(common)}):")
    for t in sorted(common):
        cols_a = schema_a[t]
        cols_b = schema_b[t]
        diff = []

        for col in set(cols_a.keys()) | set(cols_b.keys()):
            if col not in cols_a:
                diff.append(f"      + {col} (только в '{name_b}')")
            elif col not in cols_b:
                diff.append(f"      - {col} (только в '{name_a}')")
            else:
                ca, cb = cols_a[col], cols_b[col]
                if (ca["type"], ca["nullable"], ca["default"]) != (
                    cb["type"],
                    cb["nullable"],
                    cb["default"],
                ):
                    diff.append(
                        f"      ~ {col}: "
                        f"'{name_a}'={ca['type']}(nullable={ca['nullable']}, default={ca['default']}) "
                        f"vs "
                        f"'{name_b}'={cb['type']}(nullable={cb['nullable']}, default={cb['default']})"
                    )

        if diff:
            print(f"    {t}:")
            for line in diff:
                print(line)

    unchanged = sum(
        1 for t in common
        if schema_a[t] == schema_b[t]
    )
    print(f"\n  Без изменений: {unchanged} таблиц")


def _compare_full(name_a, schema_a, name_b, schema_b):
    _compare_tables_list(name_a, schema_a, name_b, schema_b)
    _compare_common_tables(name_a, schema_a, name_b, schema_b)


def compare_interactive():
    try:
        dbs = asyncio.run(fetch_databases(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD
        ))
    except (ConnectionError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
        print(f"Ошибка подключения: {e}")
        return

    print("Базы данных на сервере:")
    for i, db in enumerate(dbs, 1):
        print(f"  {i}. {db}")

    if len(dbs) < 2:
        print("Нужно минимум 2 базы для сравнения.")
        return

    db_a = _pick_db("Выберите первую БД (номер): ", dbs)
    db_b = _pick_db("Выберите вторую БД (номер): ", dbs)

    if db_a == db_b:
        print("Выбрана одна и та же база. Сравнение не имеет смысла.")
        return

    print("\nРежим:")
    print("  1. Сравнить всю структуру")
    print("  2. Сравнить таблицы")
    print("  3. Сверить одноимённые таблицы")
    while True:
        mode = input("Выберите режим (1-3): ").strip()
        if mode in ("1", "2", "3"):
            break
        print("Ошибка: введите 1-3.")

    print(f"\nСравниваю '{db_a}' и '{db_b}'...")
    try:
        schema_a, schema_b = asyncio.run(
            _fetch_pair(db_a, db_b)
        )
    except (asyncpg.PostgresError, asyncio.TimeoutError) as e:
        print(f"Ошибка при получении схемы: {e}")
        return

    _normalize_schema(schema_a)
    _normalize_schema(schema_b)

    print(f"\n{'='*60}")
    print(f"Сравнение: '{db_a}' vs '{db_b}'")
    print(f"{'='*60}")

    if mode == "1":
        _compare_full(db_a, schema_a, db_b, schema_b)
    elif mode == "2":
        _compare_tables_list(db_a, schema_a, db_b, schema_b)
    elif mode == "3":
        _compare_common_tables(db_a, schema_a, db_b, schema_b)
    print()


async def _fetch_pair(db_a, db_b):
    schema_a = await _fetch_schema(DB_HOST, DB_USER, DB_PASSWORD, db_a)
    schema_b = await _fetch_schema(DB_HOST, DB_USER, DB_PASSWORD, db_b)
    return schema_a, schema_b


if __name__ == "__main__":
    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        print("Ошибка: проверьте connection в settings.yaml и DB_PASSWORD в .env")
    else:
        compare_interactive()
