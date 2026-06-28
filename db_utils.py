import asyncio
import os
import glob
import asyncpg


async def _try_connect(*, host, user, password, database):
    conn = await asyncpg.connect(
        host=host, user=user, password=password, database=database, timeout=5
    )
    rows = await conn.fetch(
        "SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false ORDER BY datname"
    )
    await conn.close()
    return [row["datname"] for row in rows]


async def fetch_databases(*, host, user, password):
    for db in ("postgres", "template1"):
        try:
            return await _try_connect(
                host=host, user=user, password=password, database=db
            )
        except (asyncpg.PostgresError, asyncio.TimeoutError, ConnectionError):
            continue
    raise ConnectionError(
        f"Не удалось подключиться к серверу {host}. "
        "Проверьте учётные данные и доступность сервера."
    )


def rotate_backups(db_name, keep_count, backup_dir="backup"):
    if keep_count <= 0:
        return
    pattern = os.path.join(backup_dir, f"{db_name}_backup_*.dump")
    files = sorted(glob.glob(pattern))
    if len(files) <= keep_count:
        return
    for f in files[:-keep_count]:
        os.remove(f)
