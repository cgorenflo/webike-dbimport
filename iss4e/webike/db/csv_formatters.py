import ast
import csv
from abc import ABCMeta, abstractmethod


class Formatter(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def format(self, reader: csv.DictReader): pass


class V1Formatter(Formatter):
    pass


class V2Formatter(Formatter):
    pass


class V3Formatter(Formatter):
    def format(self, reader: csv.DictReader) -> dict:
        """
        :param reader: log file data
        :returns formatted data
        """
        return {"points": [{"measurement": "sensor_data",
                            "tags": {"imei": row.pop("IMEI"),
                                     "code_version": ast.literal_eval(row.pop("code_version"))},
                            "time": row.pop("timestamp"),
                            "fields": self._get_fields_with_correct_data_type(row)
                            } for row in reader if self._filter_for_correct_log_format(row)]}

    def _get_fields_with_correct_data_type(self, row: dict) -> dict:
        return dict([key, ast.literal_eval(value.title())] for key, value in row.items() if value)

    def _filter_for_correct_log_format(self, row: dict) -> bool:
        return "IMEI" in row.keys()
