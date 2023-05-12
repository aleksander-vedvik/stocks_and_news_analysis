from models.stock_result import StockResult
from tools.console_printer import ConsolePrinter
from tools.number_or_default import int_or_default, float_or_default


class Stock:
    def __init__(self):
        self.name = ""
        self.results: list[StockResult] = []
        self.index = 0
        self.currentArray = ""
        self.complete = False
        self.number_of_value = 0

    def parse_line(self, line: str):
        line = line.strip()
        if line.find("\"symbol\":") != -1:
            self.name = line.split(": ")[1].strip(',').strip('\"')
            return
        if self.complete:
            return
        if self.currentArray == "":
            self.find_current_array(line)
            return

        if line.find("]") == -1:
            self.store_value(line)
            return

        if line.find("]") != -1 and self.currentArray == "low":
            self.complete = True
            return

        self.currentArray = ""
        self.index = 0

    def store_value(self, line: str):
        line = line.strip(',')
        if self.currentArray == "timestamp":
            value = int_or_default(line)
            self.results.append(StockResult())
            self.results[self.index].timestamp = value
            self.index += 1
            return

        if self.currentArray == "open":
            value = float_or_default(line)
            self.results[self.index].open = value
            self.index += 1
            return
        if self.currentArray == "close":
            value = float_or_default(line)
            self.results[self.index].close = value
            self.index += 1
            return
        if self.currentArray == "volume":
            value = int_or_default(line)
            self.results[self.index].volume = value
            self.index += 1
            return
        if self.currentArray == "high":
            value = float_or_default(line)
            self.results[self.index].high = value
            self.index += 1
            return
        if self.currentArray == "low":
            value = float_or_default(line)
            self.results[self.index].low = value
            self.index += 1

    def find_current_array(self, line: str):
        if line.find("\"timestamp\": [") != -1:
            self.currentArray = "timestamp"
            return
        if line.find("\"open\": [") != -1:
            self.currentArray = "open"
            return
        if line.find("\"close\": [") != -1:
            self.currentArray = "close"
            return
        if line.find("\"volume\": [") != -1:
            self.currentArray = "volume"
            return
        if line.find("\"high\": [") != -1:
            self.currentArray = "high"
            return
        if line.find("\"low\": [") != -1:
            self.currentArray = "low"
            return
