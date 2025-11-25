import json
import time
from datetime import datetime
import os

MESSAGE_RATE_WINDOW = int(os.getenv("MESSAGE_RATE_WINDOW", "10"))
MESSAGE_RATE_THRESHOLD = int(os.getenv("MESSAGE_RATE_THRESHOLD", "1000"))
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"


class AlpacaMessageHandler:
    def __init__(self, controller):
        self.controller = controller
        self.message_times = []  # timestamps for rate calculation

    # ---------------------------------------------------------
    # Debug printing helper
    # ---------------------------------------------------------
    def debug(self, *args):
        if DEBUG_MODE:
            print("[DEBUG]", *args)

    # ---------------------------------------------------------
    # Rate monitoring
    # ---------------------------------------------------------
    def _track_rate(self):
        now = time.time()
        self.message_times.append(now)

        # keep only last N seconds
        cutoff = now - MESSAGE_RATE_WINDOW
        self.message_times = [t for t in self.message_times if t >= cutoff]

        rate = len(self.message_times)

        if rate > MESSAGE_RATE_THRESHOLD:
            print(f"[WARN] High message rate detected: {rate} msgs in last {MESSAGE_RATE_WINDOW}s")

    # ---------------------------------------------------------
    # Message handler (called from alpaca_news_client)
    # ---------------------------------------------------------
    async def handle(self, raw_message):
        """
        Parse raw websocket message (string), filter for news,
        augment metadata, rate-track, and forward to controller.
        """
        self.debug("Raw message received:", raw_message)

        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError:
            print("[WARN] Failed to decode JSON message")
            return

        messages = data if isinstance(data, list) else [data]

        for msg in messages:
            msg_type = msg.get("T")
            if msg_type != "n":  
                continue  # ignore non-news messages

            msg["ingested_at"] = datetime.utcnow().isoformat()

            self._track_rate()

            # Pass to PipelineController
            try:
                await self.controller.handle_news(msg)
            except Exception as e:
                print("[ERROR] Failed to process news:", e)
                continue
