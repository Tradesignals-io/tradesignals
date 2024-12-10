from option_trades.flink_sql import OPTION_TRADES_TABLE_DDL
from option_trades.pipeline import OptionTradesPipeline

__all__ = ["OPTION_TRADES_TABLE_DDL", "OptionTradesPipeline"]

if __name__ == "__main__":
    from tornado.ioloop import IOLoop
    from tornado.platform.asyncio import AsyncIOMainLoop

    AsyncIOMainLoop().install()
    pipeline = OptionTradesPipeline("option_trades", debug=False)
    IOLoop.current().run_sync(pipeline.start)