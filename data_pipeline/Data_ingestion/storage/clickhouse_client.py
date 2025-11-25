import asyncio
import os
import clickhouse_connect
from utils.logging_config import get_logger
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("CH_HOST", "localhost")
port = int(os.getenv("CH_PORT", "8123"))
user = os.getenv("CH_USER", "default")
password = os.getenv("CH_PASSWORD", "")

logger = get_logger("ch-client")


class ClickHouseClient:
    """
    Async ClickHouse client using clickhouse-connect.
    Built for reliability inside an async ingestion pipeline.
    """

    def __init__(self):
        

        self.client = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password

        logger.info(f"ClickHouseClient initialized for {host}:{port}")