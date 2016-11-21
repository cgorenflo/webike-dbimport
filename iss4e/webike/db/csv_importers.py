import ast
import os
from abc import ABCMeta, abstractmethod
from csv import DictReader
from io import TextIOWrapper
from typing import Iterator, Tuple

NEW_IMPORT_FORMAT_CODE_VERSION = 21


class CSVImporter(object):
    __metaclass__ = ABCMeta

    def read_logs(self, log_file_paths: Iterator[Tuple[str, str]]) -> Iterator[Tuple[str, str, dict]]:
        """
        :param log_file_paths: An iterator over directories and log file names
        :returns an iterator over directories, log file names of their data
        """

        for directory, file_name in log_file_paths:
            with open(os.path.join(directory, file_name)) as csv_file:
                reader = self._get_reader(csv_file, directory)
                data = self._format(reader)
                if data["points"]:
                    yield directory, file_name, data

    def _format(self, reader: DictReader) -> dict:
        """
        :param reader: log file data
        :returns formatted data
        """
        return {"points": [{"measurement": "sensor_data",
                            "tags": {"imei": self._get_imei(row),
                                     "code_version": ast.literal_eval(row.pop("code_version"))},
                            "time": row.pop("timestamp"),
                            "fields": self._get_fields_with_correct_data_type(row)
                            } for row in reader if self._filter_for_correct_log_format(row)]}

    def _get_fields_with_correct_data_type(self, row: dict) -> dict:
        return dict([key, self._get_value(value)] for key, value in row.items() if
                    self._filter_for_correct_value_format(value))

    def _get_value(self, value):
        try:
            return ast.literal_eval(value.title())
        except ValueError:
            return value

    @abstractmethod
    def _get_imei(self, row: dict) -> str:
        pass

    @abstractmethod
    def _filter_for_correct_log_format(self, row: dict) -> bool:
        pass

    @abstractmethod
    def _filter_for_correct_value_format(self, value: str) -> bool:
        pass

    @abstractmethod
    def _get_reader(self, csv_file: TextIOWrapper, file_directory: str) -> DictReader:
        pass


class LegacyImporter(CSVImporter):
    def _filter_for_correct_value_format(self, value: str) -> bool:
        print(value)
        return value and value.lower() != "null" and value.lower() != "nan"

    def _get_imei(self, row: dict) -> str:
        return self.imei

    def _get_reader(self, csv_file: TextIOWrapper, file_directory: str) -> DictReader:
        self.imei = os.path.basename(os.path.normpath(file_directory))
        return DictReader(csv_file, fieldnames=["timestamp",
                                                "class",
                                                "code_version",
                                                "latitude",
                                                "longitude",
                                                "network_latitude",
                                                "network_longitude",
                                                "acceleration_x",
                                                "acceleration_y",
                                                "acceleration_z",
                                                "magnetic_field_x",
                                                "magnetic_field_y",
                                                "magnetic_field_z",
                                                "gyroscope_x",
                                                "gyroscope_y",
                                                "gyroscope_z",
                                                "atmospheric_pressure",
                                                "light_level",
                                                "gravitational_acceleration",
                                                "linear_acceleration_x",
                                                "linear_acceleration_y",
                                                "linear_acceleration_z",
                                                "step_count",
                                                "battery_temperature",
                                                "ambient_temperature",
                                                "voltage",
                                                "charging_current",
                                                "significant_motion"
                                                "proximity_sensor",
                                                "phone_ip",
                                                "phone_battery_state",
                                                "discharge_current"])

    def _filter_for_correct_log_format(self, row: dict) -> bool:
        return ast.literal_eval(row["code_version"]) < NEW_IMPORT_FORMAT_CODE_VERSION


class WellFormedCSVImporter(CSVImporter):
    def _filter_for_correct_value_format(self, value: str) -> bool:
        return value

    def _get_imei(self, row: dict) -> str:
        return row.pop("IMEI")

    def _get_reader(self, csv_file: TextIOWrapper, file_directory: str) -> DictReader:
        return DictReader(csv_file)

    def _filter_for_correct_log_format(self, row: dict) -> bool:
        return "code_version" in row.keys() and ast.literal_eval(row["code_version"]) >= NEW_IMPORT_FORMAT_CODE_VERSION
