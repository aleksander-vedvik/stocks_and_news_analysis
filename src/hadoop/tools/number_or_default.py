def int_or_default(value: str) -> int:
    try:
        return int(value)
    except:
        return 0


def float_or_default(value: str) -> float:
    try:
        return float(value)
    except:
        return 0.0
