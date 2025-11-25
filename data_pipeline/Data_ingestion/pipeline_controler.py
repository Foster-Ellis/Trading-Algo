import asyncio


from ingestion.alpaca_news_client import AlpacaNewsClient
from processing.hashers import AlpacaNewsHasher
from utils.logging_config import get_logger


logger = get_logger("pipeline")

class PipelineController:
    def __init__(self):
        #self.writer = ClickHouseWriter()
        logger.info("PipelineController initialized.")
        self.ws_client = AlpacaNewsClient(controller=self)

    async def handle_news(self, msg: dict):
        """
        Called by alpaca_news_client whenever new news arrives.
        Pipeline = hash → (future) CH write → logs
        """
        # 1. Compute hash
        hash_value = AlpacaNewsHasher.compute(msg)
        logger.info(f"News received | id={msg.get('id')} | hash={hash_value[:12]}")




async def main():
    controller = PipelineController()
    await controller.ws_client.run()


if __name__ == "__main__":
    asyncio.run(main())