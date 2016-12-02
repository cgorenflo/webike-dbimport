import ast
import logging
import os
from abc import ABCMeta, abstractmethod
from csv import DictReader
from io import TextIOWrapper
from typing import Iterator, Tuple

# noinspection PyPep8Naming
from iss4e.util import BraceMessage as __

from iss4e.webike.db.classes import *
from iss4e.webike.db.date_time import DateTime

NEW_IMPORT_FORMAT_CODE_VERSION = 21
logger = logging.getLogger("iss4e.webike.db")


class CSVImporter(object):
    __metaclass__ = ABCMeta

    def read_logs(self, directory: Directory, files: Iterator[File]) -> Iterator[Tuple[Directory, File, Data]]:
        """
        :returns an iterator over directories, log file names of their data
        """
        logger.info("Start reading log files")
        for file_name in files:
            logger.debug(__("Read log file {file} in directory {dir}", file=file_name, dir=directory.name))
            with open(os.path.join(directory.abs_path, file_name)) as csv_file:
                reader = self._get_reader(csv_file, directory.name)
                data = self._format(reader)
                if data["points"]:
                    yield directory, file_name, data
                else:
                    logger.warning(
                        __("No sensor data read from file {file} in directory {dir}", file=file_name,
                           dir=directory.name))
                    yield directory, file_name, None

        return ()

    def _format(self, reader: DictReader) -> dict:
        """
        :param reader: log file data
        :returns formatted data
        """

        logger.debug("Formatting row")

        return {"points": [{"measurement": "sensor_data",
                            "tags": {"imei": self._get_imei(row)},
                            "time": DateTime.from_string(row.pop("timestamp"), 'Canada/Eastern').utc_time,
                            "fields": self._get_fields_with_correct_data_type(row)
                            } for row in reader if self._filter_for_correct_log_format(row)]}

    def _get_fields_with_correct_data_type(self, row: dict) -> dict:
        return dict((key, self._get_value(key, value)) for key, value in row.items() if
                    self._filter_for_correct_value_format(value))

    @staticmethod
    def _get_value(key: str, value: str):
        try:
            # parse boolean values to python upper case spelling with str.title()
            parsed_value = ast.literal_eval(value.title())

            # some code version entries are in a float format, int is expected
            if key == "code_version":
                parsed_value = int(parsed_value)

            return parsed_value
        except (ValueError, SyntaxError):
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
    def _get_reader(self, csv_file: TextIOWrapper, directory_name: str) -> DictReader:
        pass


class LegacyImporter(CSVImporter):
    def _filter_for_correct_value_format(self, value: str) -> bool:
        if value and value.lower() != "null" and value.lower() != "nan":
            return True
        else:
            logger.debug(__("Value {value} denied", value=value))

    def _get_imei(self, row: dict) -> str:
        return self.imei

    def _get_reader(self, csv_file: TextIOWrapper, directory_name: str) -> DictReader:
        self.imei = directory_name
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
                                                "significant_motion",
                                                "proximity_sensor",
                                                "phone_ip",
                                                "phone_battery_state",
                                                "discharge_current"])

    def _filter_for_correct_log_format(self, row: dict) -> bool:
        try:
            logger.debug(__("Check code version filter for row: {row}", row=row))
            # old log files contain rows with written log messages instead of sensor data,
            # so there might be an unparsable string in the 'code_version' field
            if ast.literal_eval(row["code_version"]) < NEW_IMPORT_FORMAT_CODE_VERSION:
                row.pop("class")
                row.pop("step_count")
                row.pop("significant_motion")
                row.pop("phone_ip")
                return True
            else:
                logger.debug(__("Code version is {version}", version=row["code_version"]))
                return False
        except (ValueError, SyntaxError):
            logger.debug(__("'code_version' field could not be parsed. Value: {value}", value=row["code_version"]))
            return False


class WellFormedCSVImporter(CSVImporter):
    def _filter_for_correct_value_format(self, value: str) -> bool:
        return bool(value)

    def _get_imei(self, row: dict) -> str:
        return row.pop("IMEI")

    def _get_reader(self, csv_file: TextIOWrapper, directory_name: str) -> DictReader:
        return DictReader(csv_file)

    def _filter_for_correct_log_format(self, row: dict) -> bool:
        # old logs don't have a header, so there will be no 'code_version' field
        return "code_version" in row.keys() and ast.literal_eval(row["code_version"]) >= NEW_IMPORT_FORMAT_CODE_VERSION
