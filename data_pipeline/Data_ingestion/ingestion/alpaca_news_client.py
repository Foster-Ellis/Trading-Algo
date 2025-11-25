import asyncio
import json
import os
import random
import websockets
from dotenv import load_dotenv

from ingestion.alpaca_message_handler import AlpacaMessageHandler
from utils.logging_config import get_logger

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
NEWS_URL = os.getenv("APCA_NEWS_STREAM_URL")

RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "3"))
MAX_RECONNECT_DELAY = int(os.getenv("MAX_RECONNECT_DELAY", "60"))

WS_PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "20"))
WS_PING_TIMEOUT = int(os.getenv("WS_PING_TIMEOUT", "10"))
WS_CLOSE_TIMEOUT = int(os.getenv("WS_CLOSE_TIMEOUT", "10"))

logger = get_logger("ws-client")


class AlpacaNewsClient:
    def __init__(self, controller):
        self.controller = controller
        self.handler = AlpacaMessageHandler(controller)
        self.ws = None
        self.running = True
        self.retry_count = 0

    def debug(self, *args):
        logger.debug(" ".join(str(a) for a in args))

    # --------------------------------------------------------
    # WebSocket events
    # --------------------------------------------------------
    async def on_open(self):
        self.debug("WS opened → Authenticating")

        try:
            await self.ws.send(json.dumps({
                "action": "auth",
                "key": API_KEY,
                "secret": API_SECRET
            }))

            auth_response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            logger.debug(f"Auth response: {auth_response}")

        except asyncio.TimeoutError:
            logger.error("Authentication timeout")
            raise

        try:
            await self.ws.send(json.dumps({
                "action": "subscribe",
                "news": ["*"]
            }))

            sub_response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            logger.debug(f"Subscription response: {sub_response}")

        except asyncio.TimeoutError:
            logger.error("Subscription timeout")
            raise

        logger.info("Authenticated + Subscribed to *")

    async def on_message(self, raw):
        await self.handler.handle(raw)

    async def on_error(self, error):
        logger.error(f"WS error: {error}")

    async def on_close(self, code=None, reason=None):
        logger.info(f"WS closed | code={code} reason={reason}")

    async def run(self):
        while self.running:
            try:
                logger.debug(f"WS connecting → {NEWS_URL}")

                async with websockets.connect(
                    NEWS_URL,
                    ping_interval=WS_PING_INTERVAL,
                    ping_timeout=WS_PING_TIMEOUT,
                    close_timeout=WS_CLOSE_TIMEOUT,
                ) as socket:

                    self.ws = socket
                    self.retry_count = 0

                    await self.on_open()

                    async for message in self.ws:
                        await self.on_message(message)

            except Exception as e:
                await self.on_error(e)

                self.retry_count += 1
                delay = min(RECONNECT_DELAY * (2 ** (self.retry_count - 1)), MAX_RECONNECT_DELAY)
                delay += random.uniform(0, 1.0)

                logger.warning(
                    f"WS reconnecting in {delay:.1f}s "
                    f"(attempt #{self.retry_count})"
                )
                await asyncio.sleep(delay)

        await self.on_close()

