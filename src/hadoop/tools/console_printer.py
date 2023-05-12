import sys
class ConsolePrinter:
    @staticmethod
    def print(value: str):
        sys.stdout.write(bytearray(value.encode()))