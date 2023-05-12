from models.stock import Stock
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol
from tools.console_printer import ConsolePrinter
import sys
import logging


class MRStocks(MRJob):
    DIRS = ['models', 'tools']
    OUTPUT_PROTOCOL = RawProtocol

    def mapper_init(self):
        self.stock = Stock()

    def mapper_raw(self, filename, _):
        try:
            with open(filename, "r") as file:
                for line in file:
                    self.stock.parse_line(line)
                    if self.stock.complete:
                        break
            if self.stock.complete:
                for i in range(len(self.stock.results)):
                    yield None, self.stock.results[i].to_csv(self.stock.name)
        except:
            ConsolePrinter.print(f"Failed at file {filename}")

    def reducer(self, key, values):
        for val in values:
            yield None, val


if __name__ == "__main__":
    MRStocks.run()
