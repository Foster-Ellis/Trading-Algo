import asyncio
import json
import websockets
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

NEWS_URL = os.getenv("Alpaca_News_URL")

class AlpacaNewsClient:
    def __init__(self, controller):
        """
        controller: instance of PipelineController
        """
        self.controller = controller
        self.ws = None
        self.running = True

        # ----------------------------------------------------
    #  WEBSOCKET EVENTS
    # ----------------------------------------------------
    async def on_open(self):
        print("[WS] Connected → Authenticating...")

        await self.ws.send(json.dumps({
            "action": "auth",
            "key": API_KEY,
            "secret": API_SECRET
        }))

        await self.ws.send(json.dumps({
            "action": "subscribe",
            "news": ["*"]
        }))

    async def on_message(self, message):
        print("MESSAGE:", message)
        """
        Called for every WebSocket message.
        Filters news messages and forwards them to controller.
        """
        try:
            msg = json.loads(message)[0]
            if msg.get("T") != "n":
                #print("MESSAGE:", message)
                return  # ignore non-news messages
        except Exception:
            return

        msg["ingested_at"] = datetime.utcnow().isoformat()

        # Send parsed message to controller
        await self.controller.handle_news(msg)

    async def on_error(self, error):
        print("[WS ERROR]", error)

    async def on_close(self):
        print("[WS] Connection closed")


    async def run(self):
        """
        Connect → authenticate → subscribe → process messages.
        Auto-reconnect on failure.
        """
        while self.running:
            try:
                async with websockets.connect(NEWS_URL) as socket:
                    self.ws = socket

                    await self.on_open()

                    async for message in self.ws:
                        await self.on_message(message)

            except Exception as e:
                await self.on_error(e)
                print("[WS] Reconnecting in 3 seconds...")
                await asyncio.sleep(3)

        await self.on_close()


if __name__ == "__main__":
    class DummyController:
        async def handle_news(self, msg):
            print("[TEST] Parsed news:")
            print(json.dumps(msg, indent=2))

    dummy = DummyController()
    client = AlpacaNewsClient(controller=dummy)
    asyncio.run(client.run())