from constants import *
import struct
from src.operators.convertors import *


def get_roll_input(signal_hex_values):
    # - ID 0x7e3, fields
    # - roll axis input as a 2 byte signed integer in `[-32768, 32767]` that cor0responds to a commanded angle in `[-30, 30]` degrees
    return radian_to_degree(dec_from_hex(signal_hex_values))


def get_pitch_input(signal_hex_values):
    # - ID 0x7e3, fields
    # - pitch axis input as a 2 byte signed integer in `[-32768, 32767]` that corresponds to a commanded angle in `[-30, 30]` degrees
    return radian_to_degree(dec_from_hex(signal_hex_values))


def get_yaw_input(signal_hex_values):
    # - ID 0x7e3, fields
    # - yaw axis input as a 2 byte signed integer in `[-32768, 32767]` that corresponds to a commanded angular rate in `[-60, 60]` degrees/s
    return radian_to_degree(dec_from_hex(signal_hex_values))


def get_hover_throttle_input(signal_hex_values):
    # - ID 0x7e3, which contains the following fields (in this order)

    # - hover throttle as a 2 byte unsigned integer in `[0, 65535]` that corresponds to a throttle percentage
    return dec_from_hex(signal_hex_values, signed=False)


def get_propspin_input(signal_hex_values):
    # - ID 0x7e4, fields
    # - propspin switch as a 1 byte unsigned integer in `[0, 255]` that corresponds to a bool

    return float(bool(int(signal_hex_values, 16)))


def get_pusher_throttle_input(signal_hex_values):
    # - ID 0x7e4, fields
    # - pusher throttle as a 2 byte signed integer in `[-32768, 32767]` that corresponds to +/- percentage, w/ 0 being neutral

    return dec_from_hex(signal_hex_values)


def get_IMU_pitch_angle(signal_hex_values):
    # Additionally, inertial measurement unit (IMU):
    # - ID 0x1 contains the pitch angle (in radians) as a 4 byte float, followed by some data we can ignore
    return radian_to_degree(
        struct.unpack(
            "!f",
            bytes.fromhex(signal_hex_values),
        )[0]
    )


def get_IMU_pitch_rate(signal_hex_values):
    # Additionally, inertial measurement unit (IMU):
    # - ID 0x2 contains the pitch rate (in radians/s) as a 4 byte float, followed by some data we can ignore
    return radian_to_degree(
        struct.unpack(
            "!f",
            bytes.fromhex(signal_hex_values),
        )[0]
    )


def get_IMU_roll_angle(signal_hex_values):
    # Additionally, inertial measurement unit (IMU):
    # - ID 0x3 contains the roll angle (in radians) as a 4 byte float, followed by some data we can ignore
    return radian_to_degree(
        struct.unpack(
            "!f",
            bytes.fromhex(signal_hex_values),
        )[0]
    )


def get_IMU_roll_rate(signal_hex_values):
    # Additionally, inertial measurement unit (IMU):
    # - ID 0x4 contains the roll rate (in radians/s) as a 4 byte float, followed by some data we can ignore
    return radian_to_degree(
        struct.unpack(
            "!f",
            bytes.fromhex(signal_hex_values),
        )[0]
    )


def get_IMU_yaw_angle(signal_hex_values):
    # Additionally, inertial measurement unit (IMU):
    # - ID 0x5 contains the yaw angle (in radians) as a 4 byte float, followed by some data we can ignore
    return radian_to_degree(
        struct.unpack(
            "!f",
            bytes.fromhex(signal_hex_values),
        )[0]
    )


def get_IMU_yaw_rate(signal_hex_values):
    # Additionally, inertial measurement unit (IMU):
    # - ID 0x6 contains the yaw rate (in radians/s) as a 4 byte float, followed by some data we can ignore
    return radian_to_degree(
        struct.unpack(
            "!f",
            bytes.fromhex(signal_hex_values),
        )[0]
    )


def signal_aggregator(signal):
    Timestamp = signal[0]
    Bus = signal[1]
    if signal[2] == "7E3":
        Singal = ["roll_input", "pitch_input", "yaw_input", "hover_throttle_input"]
        return [
            Timestamp,
            Bus,
            Singal,
            [
                get_roll_input(signal[4] + signal[5]),
                get_pitch_input(signal[6] + signal[7]),
                get_yaw_input(signal[8] + signal[9]),
                get_hover_throttle_input(signal[10] + signal[11]),
            ],
        ]

    if signal[2] == "7E4":
        Singal = ["propspin_input", "pusher_throttle_input"]
        return [
            Timestamp,
            Bus,
            Singal,
            [
                get_propspin_input(signal[4]),
                get_pusher_throttle_input(signal[5] + signal[6]),
            ],
        ]

    if signal[2] == "1":
        return [
            Timestamp,
            Bus,
            "pitch_angle",
            get_IMU_pitch_angle(signal[4] + signal[5] + signal[6] + signal[7]),
        ]

    if signal[2] == "2":
        return [
            Timestamp,
            Bus,
            "pitch_rate",
            get_IMU_pitch_rate(signal[4] + signal[5] + signal[6] + signal[7]),
        ]

    if signal[2] == "3":
        return [
            Timestamp,
            Bus,
            "roll_angle",
            get_IMU_roll_angle(signal[4] + signal[5] + signal[6] + signal[7]),
        ]

    if signal[2] == "4":
        return [
            Timestamp,
            Bus,
            "roll_rate",
            get_IMU_roll_rate(signal[4] + signal[5] + signal[6] + signal[7]),
        ]

    if signal[2] == "5":
        return [
            Timestamp,
            Bus,
            "yaw_angle",
            get_IMU_yaw_angle(signal[4] + signal[5] + signal[6] + signal[7]),
        ]

    if signal[2] == "6":
        return [
            Timestamp,
            Bus,
            "yaw_rate",
            get_IMU_yaw_rate(signal[4] + signal[5] + signal[6] + signal[7]),
        ]


def get_signal_value_pairs(signal):
    if signal[2] == "7E3":
        return [
            ["roll_input", get_roll_input(signal[4] + signal[5])],
            ["pitch_input", get_pitch_input(signal[6] + signal[7])],
            ["yaw_input", get_yaw_input(signal[8] + signal[9])],
            ["hover_throttle_input", get_hover_throttle_input(signal[10] + signal[11])],
        ]

    if signal[2] == "7E4":
        return [
            ["propspin_input", get_propspin_input(signal[4])],
            ["pusher_throttle_input", get_pusher_throttle_input(signal[5] + signal[6])],
        ]

    if signal[2] == "1":
        return [
            [
                "pitch_angle",
                get_IMU_pitch_angle(signal[4] + signal[5] + signal[6] + signal[7]),
            ]
        ]

    if signal[2] == "2":
        return [
            [
                "pitch_rate",
                get_IMU_pitch_rate(signal[4] + signal[5] + signal[6] + signal[7]),
            ]
        ]

    if signal[2] == "3":
        return [
            [
                "roll_angle",
                get_IMU_roll_angle(signal[4] + signal[5] + signal[6] + signal[7]),
            ]
        ]

    if signal[2] == "4":
        return [
            [
                "roll_rate",
                get_IMU_roll_rate(signal[4] + signal[5] + signal[6] + signal[7]),
            ]
        ]

    if signal[2] == "5":
        return [
            [
                "yaw_angle",
                get_IMU_yaw_angle(signal[4] + signal[5] + signal[6] + signal[7]),
            ]
        ]

    if signal[2] == "6":
        return [
            [
                "yaw_rate",
                get_IMU_yaw_rate(signal[4] + signal[5] + signal[6] + signal[7]),
            ]
        ]
