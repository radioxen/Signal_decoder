import numpy as np


def radian_to_degree(x: int):

    return np.degrees(np.arctan(np.tan(x)))


def dec_from_hex(hex_value, bytes=2, signed=True):
    if not signed:
        return int(hex_value, base=16)
    else:
        bits = bytes * 8
        value = int(hex_value, 16)
        if value & (1 << (bits - 1)):
            value -= 1 << bits
        return value
