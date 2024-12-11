from .pipeline import EquityTradesPipeline
    
__all__ = ["EquityTradesPipeline"]

if __name__ == "__main__":
    pipeline = EquityTradesPipeline("equity_trades", debug=False)
    pipeline.start()
