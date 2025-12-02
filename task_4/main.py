import asyncio
from pathlib import Path

from aiochclient import ChClient
from aiohttp import ClientSession
from loguru import logger

from task_3.config import settings


async def init_test_data(client: ChClient):
    logger.info("Initializing test table and data...")

    ddl = """
    CREATE TABLE IF NOT EXISTS phrases_views
    (
        dt          DateTime,
        campaign_id Int32 comment 'Идентификатор рекламной кампании',
        phrase      String comment 'Поисковой запрос',
        views       Int32 comment 'Кумулятивное (суммарное) количество просмотров по поисковому запросу за всё время'
    ) engine = ReplacingMergeTree ORDER BY (dt, campaign_id, phrase);
    """
    await client.execute(ddl)

    await client.execute("TRUNCATE TABLE phrases_views")

    insert_sql = """
    INSERT INTO test.phrases_views (dt, campaign_id, phrase, views)
    VALUES ('2025-01-01 11:50:00', 1111111, 'платье', 0),
    ('2025-01-01 12:00:00', 1111111, 'платье', 1),
    ('2025-01-01 12:10:00', 1111111, 'платье', 1),
    ('2025-01-01 12:20:00', 1111111, 'платье', 1),
    ('2025-01-01 12:30:00', 1111111, 'платье', 1),
    ('2025-01-01 12:40:00', 1111111, 'платье', 1),
    ('2025-01-01 12:50:00', 1111111, 'платье', 1),
    ('2025-01-01 13:00:00', 1111111, 'платье', 1),
    ('2025-01-01 13:10:00', 1111111, 'платье', 1),
    ('2025-01-01 13:20:00', 1111111, 'платье', 1),
    ('2025-01-01 13:30:00', 1111111, 'платье', 2),
    ('2025-01-01 13:40:00', 1111111, 'платье', 3),
    ('2025-01-01 13:50:00', 1111111, 'платье', 5),
    ('2025-01-01 14:00:00', 1111111, 'платье', 5),
    ('2025-01-01 14:10:00', 1111111, 'платье', 6),
    ('2025-01-01 14:20:00', 1111111, 'платье', 8),
    ('2025-01-01 14:30:00', 1111111, 'платье', 9),
    ('2025-01-01 14:40:00', 1111111, 'платье', 10),
    ('2025-01-01 14:50:00', 1111111, 'платье', 11),
    ('2025-01-01 15:00:00', 1111111, 'платье', 11),
    ('2025-01-01 15:10:00', 1111111, 'платье', 11),
    ('2025-01-01 15:20:00', 1111111, 'платье', 12),
    ('2025-01-01 15:30:00', 1111111, 'платье', 13),
    ('2025-01-01 15:40:00', 1111111, 'платье', 13),
    ('2025-01-01 15:50:00', 1111111, 'платье', 15),
    ('2025-01-01 16:00:00', 1111111, 'платье', 15);
    """
    await client.execute(insert_sql)
    logger.info("Test data inserted.")


def load_sql_query(filename: str) -> str:
    current_dir = Path(__file__).parent
    sql_path = current_dir / filename

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found at {sql_path}")

    return sql_path.read_text(encoding="utf-8")


async def main():
    session = ClientSession()
    client = ChClient(
        session=session,
        url=settings.CLICKHOUSE_URL,
        user=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DB,
    )

    try:
        await init_test_data(client)

        query = load_sql_query("query.sql")

        logger.info("Executing analytical query...")
        result = await client.fetch(query)

        print(f"{'phrase':<10} {'views_by_hour'}")

        for row in result:
            phrase = row["phrase"]
            data = row["views_by_hour"]
            print(f"{phrase:<10} {data}")

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
