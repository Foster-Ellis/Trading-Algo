import asyncio

from ingestion.alpaca_news_client import AlpacaNewsClient

class PipelineController:
    def __init__(self):
        #self.writer = ClickHouseWriter()
        self.ws_client = AlpacaNewsClient(controller=self)

    async def handle_news(self, msg):
        """
        Called by alpaca_news_client whenever new news arrives.
        """
        # hash → dataframe → write to CH
        print("[TEST] News received:", msg)


async def main():
    controller = PipelineController()
    await controller.ws_client.run()

asyncio.run(main())