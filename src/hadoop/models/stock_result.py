from datetime import datetime


class StockResult:
    def __init__(self):
        self.timestamp: int = 0
        self.open: float = 0.0
        self.close: float = 0.0
        self.volume: int = 0
        self.high: float = 0.0
        self.low: float = 0.0

    def to_csv(self, stockname: str):
        return f"{self.timestamp},{stockname},{self.open},{self.close},{self.volume},{self.high},{self.low}"
