import asyncio
import json
import os
import random
import websockets
from dotenv import load_dotenv
from ingestion.alpaca_message_handler import AlpacaMessageHandler

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
NEWS_URL = os.getenv("APCA_NEWS_STREAM_URL")

RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "3"))
MAX_RECONNECT_DELAY = int(os.getenv("MAX_RECONNECT_DELAY", "60"))

WS_PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "20"))
WS_PING_TIMEOUT = int(os.getenv("WS_PING_TIMEOUT", "10"))
WS_CLOSE_TIMEOUT = int(os.getenv("WS_CLOSE_TIMEOUT", "10"))

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"


class AlpacaNewsClient:
    def __init__(self, controller):
        self.controller = controller
        self.handler = AlpacaMessageHandler(controller)
        self.ws = None
        self.running = True
        self.retry_count = 0

    def debug(self, *args):
        if DEBUG_MODE:
            print("[DEBUG]", *args)

    # --------------------------------------------------------
    # WebSocket events
    # --------------------------------------------------------
    async def on_open(self):
        self.debug("Connected → Authenticating...")

        try:
            await self.ws.send(json.dumps({
                "action": "auth",
                "key": API_KEY,
                "secret": API_SECRET
            }))

            
            auth_response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            auth_data = json.loads(auth_response)
            print("[WS] Auth response:", auth_data)

        except asyncio.TimeoutError:
            raise Exception("Authentication timeout")
        
        try:
            await self.ws.send(json.dumps({
                "action": "subscribe",
                "news": ["*"]
            }))

            
            sub_response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            print("[WS] Subscription response:", json.loads(sub_response))

        except asyncio.TimeoutError:
            raise Exception("Subscription timeout")

        print("[WS] Authenticated + Subscribed")

    async def on_message(self, raw):
        await self.handler.handle(raw)

    async def on_error(self, error):
        print("[WS ERROR]", error)

    async def on_close(self):
        print("[WS] Connection closed")

    # --------------------------------------------------------
    # Main loop with reconnect + jitter
    # --------------------------------------------------------
    async def run(self):
        while self.running:
            try:
                self.debug(f"Connecting to {NEWS_URL}")

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
                delay += random.uniform(0, 1.0)  # jitter

                print(f"[WS] Reconnecting in {delay:.1f} seconds...")
                await asyncio.sleep(delay)

        await self.on_close()


# --------------------------------------------------------
# Standalone test mode
# --------------------------------------------------------
if __name__ == "__main__":
    class DummyController:
        async def handle_news(self, msg):
            print("[TEST] News received:")
            print(json.dumps(msg, indent=2))

    dummy = DummyController()
    client = AlpacaNewsClient(dummy)
    asyncio.run(client.run())
