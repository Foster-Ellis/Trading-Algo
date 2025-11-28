import asyncio

from ingestion.alpaca_news_client import AlpacaNewsClient
from processing.hashers import AlpacaNewsHasher
from utils.logging_config import get_logger

logger = get_logger("pipeline")


class PipelineController:
    def __init__(self):
        logger.info("PipelineController initialized.")
        self.ws_client = AlpacaNewsClient(controller=self)
        self.ch_client = 

    async def handle_news(self, msg: dict):
        logger.debug(f"Handling news ID={msg.get('id')}")

        hash_value = AlpacaNewsHasher.compute(msg)
        logger.debug(f"Hash computed: {hash_value}")

        logger.info(
            f"News received | id={msg.get('id')} | hash={hash_value[:12]}"
        )


async def main():
    controller = PipelineController()
    await controller.ws_client.run()


if __name__ == "__main__":
    asyncio.run(main())
