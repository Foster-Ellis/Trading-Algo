import os
import asyncio
import clickhouse_connect
from utils.logging_config import get_logger
from dotenv import load_dotenv

load_dotenv()

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

logger = get_logger("ch-client")


class ClickHouseClient:

    def __init__(self):
        logger.info(f"Connecting to ClickHouse at {CH_HOST}:{CH_PORT}")
        self.client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
        )

    def execute(self, query, params=None):
        return self.client.query(query, parameters=params)

    def insert(self, table, rows, columns):
        return self.client.insert(table, rows, column_names=columns)
