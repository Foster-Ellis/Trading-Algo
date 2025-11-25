import asyncio
import json
import websockets
import os
from datetime import datetime
from dotenv import load_dotenv
from collections import deque
import time

load_dotenv()

# API Credentials
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
NEWS_URL = os.getenv("APCA_NEWS_STREAM_URL")

# WebSocket Configuration
WS_RECONNECT_DELAY = int(os.getenv("WS_RECONNECT_DELAY", "3"))
WS_MAX_RECONNECT_DELAY = int(os.getenv("WS_MAX_RECONNECT_DELAY", "60"))
WS_MAX_RETRIES = int(os.getenv("WS_MAX_RETRIES", "10"))
WS_PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "20"))
WS_PING_TIMEOUT = int(os.getenv("WS_PING_TIMEOUT", "10"))
WS_CLOSE_TIMEOUT = int(os.getenv("WS_CLOSE_TIMEOUT", "10"))

# Message Rate Monitoring
WS_MESSAGE_RATE_WINDOW = int(os.getenv("WS_MESSAGE_RATE_WINDOW", "100"))
WS_MESSAGE_RATE_THRESHOLD = float(os.getenv("WS_MESSAGE_RATE_THRESHOLD", "2.0"))

# Logging
WS_DEBUG_MODE = os.getenv("WS_DEBUG_MODE", "False").lower() == "true"

class AlpacaNewsClient:
    def __init__(self, controller):
        """
        controller: instance of PipelineController
        """
        self.controller = controller
        self.ws = None
        self.running = True
        self.message_times = deque(maxlen=WS_MESSAGE_RATE_WINDOW)
        self.reconnect_delay = WS_RECONNECT_DELAY
        self.max_reconnect_delay = WS_MAX_RECONNECT_DELAY
        self.retry_count = 0
        self.max_retries = WS_MAX_RETRIES

    # ----------------------------------------------------
    #  WEBSOCKET EVENTS
    # ----------------------------------------------------
    async def on_open(self):
        print("[WS] Connected → Authenticating...")

        # Send authentication
        await self.ws.send(json.dumps({
            "action": "auth",
            "key": API_KEY,
            "secret": API_SECRET
        }))

        # Wait for auth response
        auth_response = await asyncio.wait_for(self.ws.recv(), timeout=10)
        auth_data = json.loads(auth_response)
        print(f"[WS] Auth response: {auth_data}")
        
        # Check if auth was successful
        if isinstance(auth_data, list):
            for msg in auth_data:
                if msg.get("T") == "error":
                    raise Exception(f"Auth failed: {msg.get('msg')}")
                elif msg.get("T") == "success" and msg.get("msg") == "authenticated":
                    print("[WS] ✓ Authentication successful")
        
        # Subscribe to news
        await self.ws.send(json.dumps({
            "action": "subscribe",
            "news": ["*"]
        }))
        
        # Wait for subscription confirmation
        sub_response = await asyncio.wait_for(self.ws.recv(), timeout=10)
        sub_data = json.loads(sub_response)
        print(f"[WS] Subscription response: {sub_data}")
        
        # Reset reconnect parameters on successful connection
        self.reconnect_delay = 3
        self.retry_count = 0

    async def on_message(self, message):
        """
        Called for every WebSocket message.
        Filters news messages and forwards them to controller.
        """
        # Track message frequency for monitoring
        self.message_times.append(time.time())
        
        # Alert if message rate is very high (> 50/sec sustained)
        if len(self.message_times) >= 100:
            elapsed = self.message_times[-1] - self.message_times[0]
            if elapsed < 2:  # 100 messages in < 2 seconds
                rate = 100 / elapsed
                print(f"[WARN] High message rate: {rate:.1f} msgs/sec")
        
        # Defensive JSON parsing
        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse JSON: {e}")
            print(f"[ERROR] Raw message: {message[:200]}")  # Log first 200 chars
            return
        
        # Handle both list and single message formats
        messages = data if isinstance(data, list) else [data]
        
        for msg in messages:
            # Skip non-news messages
            msg_type = msg.get("T")
            
            if msg_type == "error":
                print(f"[WS ERROR] Server error: {msg.get('msg')}")
                continue
            
            if msg_type == "success":
                print(f"[WS INFO] {msg.get('msg')}")
                continue
                
            if msg_type != "n":
                # Uncomment for debugging other message types
                print(f"[WS DEBUG] Non-news message type: {msg_type}")
                continue
            
            # Add ingestion timestamp
            msg["ingested_at"] = datetime.utcnow().isoformat()
            
            # Forward to controller with error handling
            try:
                await self.controller.handle_news(msg)
            except Exception as e:
                print(f"[ERROR] Controller failed to handle news: {e}")
                print(f"[ERROR] Message: {json.dumps(msg, indent=2)}")

    async def on_error(self, error):
        print(f"[WS ERROR] {type(error).__name__}: {error}")

    async def on_close(self):
        print("[WS] Connection closed")

    def stop(self):
        """Gracefully stop the WebSocket client"""
        print("[WS] Stopping client...")
        self.running = False

    async def run(self):
        """
        Connect → authenticate → subscribe → process messages.
        Auto-reconnect on failure with exponential backoff.
        """
        while self.running:
            # Check if we've exceeded max retries
            if self.retry_count >= self.max_retries:
                print(f"[WS] Max retries ({self.max_retries}) reached. Stopping.")
                self.running = False
                break
            
            try:
                async with websockets.connect(
                    NEWS_URL,
                    ping_interval=20,  # Send ping every 20 seconds
                    ping_timeout=10,   # Wait 10 seconds for pong
                    close_timeout=10   # Wait 10 seconds for close handshake
                ) as socket:
                    self.ws = socket
                    
                    await self.on_open()
                    
                    # Process messages until connection closes
                    async for message in self.ws:
                        await self.on_message(message)

            except asyncio.TimeoutError as e:
                await self.on_error(f"Timeout error: {e}")
                
            except websockets.exceptions.ConnectionClosed as e:
                await self.on_error(f"Connection closed: {e}")
                
            except Exception as e:
                await self.on_error(e)
            
            finally:
                # Increment retry count
                if self.running:
                    self.retry_count += 1
                    
                    # Exponential backoff
                    delay = min(self.reconnect_delay * (2 ** (self.retry_count - 1)), self.max_reconnect_delay)
                    print(f"[WS] Reconnecting in {delay} seconds... (attempt {self.retry_count}/{self.max_retries})")
                    await asyncio.sleep(delay)

        await self.on_close()


if __name__ == "__main__":
    class DummyController:
        async def handle_news(self, msg):
            print("[TEST] Parsed news:")
            print(json.dumps(msg, indent=2))

    dummy = DummyController()
    client = AlpacaNewsClient(controller=dummy)
    
    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        print("\n[MAIN] Keyboard interrupt received")
        client.stop()