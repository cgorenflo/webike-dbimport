#!/usr/bin/python3.5

import os
import re
import sys
from typing import Iterator, Tuple

import iss4e.db.influxdb as influxdb
from iss4e.util.config import load_config

import iss4e.webike.db.module_locator as module_locator
from iss4e.webike.db.csv_importers import *


def import_data(version: str = "current"):
    csv_importers = {"legacy": LegacyImporter(), "current": WellFormedCSVImporter()}

    log_file_paths = _get_log_file_paths()
    logs = csv_importers[version].read_logs(log_file_paths)
    _insert_into_db_and_archive_logs(logs)


def _get_log_file_paths() -> Iterator[Tuple[str, str]]:
    """
    :returns an iterator over directories and log file names
    """

    directory_regex_pattern = config["webike.imei_regex"]
    file_regex_pattern = config["webike.logfile_regex"]
    home, dirs, _ = next(os.walk(os.path.expanduser("~")))
    for directory in dirs:
        if re.fullmatch(directory_regex_pattern, directory):
            current_directory, _, files = next(os.walk(os.path.join(home, directory)))
            for file in files:
                if re.fullmatch(file_regex_pattern, file):
                    yield current_directory, file


def _insert_into_db_and_archive_logs(path_and_data: Iterator[Tuple[str, str, dict]]):
    """
    :param path_and_data: an iterator over directories, log file names of their data
    """
    with influxdb.connect(**config["webike.influx"]) as client:
        for directory, filename, data in path_and_data:
            client.write(data, {"db": config["webike.influx.database"]})
            _archive_log(directory, filename)


def _archive_log(directory: str, filename: str):
    os.rename(os.path.join(directory, filename), os.path.join(directory, config["webike.archive"], filename))


config = load_config(module_locator.module_path())

if len(sys.argv) > 1:
    import_data(sys.argv[1])
else:
    import_data()
