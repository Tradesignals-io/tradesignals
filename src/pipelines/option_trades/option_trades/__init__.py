"""Main entry point for the option trades pipeline."""
from tornado.ioloop import IOLoop
from tornado.platform.asyncio import AsyncIOMainLoop

from option_trades.pipeline import OptionTradesPipeline

if __name__ == "__main__":
    """Main entry point for the option trades pipeline."""
    AsyncIOMainLoop().install()
    pipeline = OptionTradesPipeline("option_trades", debug=False)
    IOLoop.current().run_sync(pipeline.start)

__all__ = ["OptionTradesPipeline"]
