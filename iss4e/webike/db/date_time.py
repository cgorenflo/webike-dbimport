import pytz
from datetime import datetime


class DateTime(object):
    def __init__(self, date_time: datetime):
        if date_time.tzinfo is None:
            raise ValueError("Give datetime has no tz_info.")
        self.__local_date_time = date_time

    @classmethod
    def from_string(cls, date_time_string: str, time_zone_str: str):
        time_zone = pytz.timezone(time_zone_str)
        return cls(time_zone.localize(datetime.strptime(date_time_string, '%Y-%m-%d %H:%M:%S.%f')))

    @classmethod
    def from_date_time(cls, date_time: datetime, time_zone: pytz.tzinfo):
        time_zone.localize(date_time)
        return cls(time_zone.localize)

    @property
    def utc_time(self) -> datetime:
        return self.__local_date_time.astimezone(pytz.utc)
