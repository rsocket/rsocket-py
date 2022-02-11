from datetime import timedelta


def to_milliseconds(period: timedelta) -> int:
    return round(period.total_seconds() * 1000) + round(period.microseconds / 1000)
