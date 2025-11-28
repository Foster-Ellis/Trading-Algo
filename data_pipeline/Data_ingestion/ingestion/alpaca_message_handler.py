import json
import time
from datetime import datetime
import asyncio
import os
from dotenv import load_dotenv

from utils.logging_config import get_logger

load_dotenv()

MESSAGE_RATE_WINDOW = int(os.getenv("MESSAGE_RATE_WINDOW"))
MESSAGE_RATE_THRESHOLD = int(os.getenv("MESSAGE_RATE_THRESHOLD"))
MESSAGE_RATE_COOLDOWN = int(os.getenv("MESSAGE_RATE_COOLDOWN"))

logger = get_logger("ws-message-handler")


class AlpacaMessageHandler:
    def __init__(self, controller):
        self.controller = controller
        self.message_times = []  # timestamps for rate calculation


    # ---------------------------------------------------------
    # Rate monitoring
    # ---------------------------------------------------------
    async def _track_rate(self):
        now = time.time()
        self.message_times.append(now)

        # keep only last N seconds
        cutoff = now - MESSAGE_RATE_WINDOW
        self.message_times = [t for t in self.message_times if t >= cutoff]

        rate = len(self.message_times)
        logger.debug(f"Message rate = {rate}/window({MESSAGE_RATE_WINDOW}s)")

        if rate > MESSAGE_RATE_THRESHOLD:
            logger.warning(
                f"High message rate: {rate} msgs in last {MESSAGE_RATE_WINDOW}s "
                f"(cooling down for {MESSAGE_RATE_COOLDOWN}s)"
            )
            if MESSAGE_RATE_COOLDOWN > 0:
                await asyncio.sleep(MESSAGE_RATE_COOLDOWN)

    # ---------------------------------------------------------
    # Message handler (called from alpaca_news_client)
    # ---------------------------------------------------------
    async def handle(self, raw_message):
        logger.debug(f"Raw WS message received: {raw_message[:300]}")

        """
        Parse raw websocket message (string), filter for news,
        augment metadata, rate-track, and forward to controller.
        """
        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.warning("Failed to decode JSON message")
            return

        messages = data if isinstance(data, list) else [data]

        for msg in messages:
            if msg.get("T") != "n":
                logger.debug("Non-news message ignored")
                continue

            logger.debug(f"News message parsed | id={msg.get('id')} symbols={msg.get('symbols')}")

            msg["ingested_at"] = datetime.utcnow().isoformat()
            await self._track_rate()

            try:
                await self.controller.handle_news(msg)
            except Exception as e:
                logger.error(f"Failed to process news message: {e}")
                continue
