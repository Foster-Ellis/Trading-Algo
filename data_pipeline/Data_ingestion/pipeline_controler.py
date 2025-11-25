import asyncio

from ingestion.alpaca_news_client import AlpacaNewsClient
from processing.hashers import AlpacaNewsHasher


class PipelineController:
    def __init__(self):
        #self.writer = ClickHouseWriter()
        self.ws_client = AlpacaNewsClient(controller=self)

    async def handle_news(self, msg: dict):
        """
        Called by alpaca_news_client whenever new news arrives.
        Pipeline = hash → (future) CH write → logs
        """
        # 1. Compute hash
        hash_value = AlpacaNewsHasher.compute(msg)

        # DEBUG / temporary output
        print("\n[PIPELINE] Incoming News Event")
        print("Message:", msg)
        print("Hash:", hash_value)


async def main():
    controller = PipelineController()
    await controller.ws_client.run()

asyncio.run(main())