#!/usr/bin/python3.5

"""Imports all sensor data log files in the imei folders into the influxdb database

Usage:
  import_data.py [-l | --legacy] [-s | --strict]

Options:
  -h --help     Show this screen.
  -l --legacy   Use legacy parser for old data formats
  -s --strict   Moves logs that could not be imported into a problem folder.
                Files stay in place if this is not set

"""
import re

import iss4e.db.influxdb as influxdb
from docopt import docopt
from iss4e.util import BraceMessage as __
from iss4e.util.config import load_config

from iss4e.webike.db import module_locator
from iss4e.webike.db.csv_importers import *


def import_data():
    logger.info("Start log file import")

    if arguments["--legacy"]:
        logger.info("Using legacy formatter")
        csv_importer = LegacyImporter()
    else:
        logger.info("Using formatter for well formed csv files")
        csv_importer = WellFormedCSVImporter()

    log_file_paths = _get_log_file_paths()
    logs = csv_importer.read_logs(log_file_paths)
    _insert_into_db_and_archive_logs(logs)

    logger.info("Import complete")


def _get_log_file_paths() -> Iterator[Tuple[str, str]]:
    """
    :returns an iterator over directories and log file names
    """
    logger.info("Start collecting log files")

    directory_regex_pattern = config["webike.imei_regex"]
    file_regex_pattern = config["webike.logfile_regex"]

    home, dirs, _ = next(os.walk(os.path.expanduser("~")))
    for directory in dirs:
        if re.fullmatch(directory_regex_pattern, directory):
            current_directory, _, files = next(os.walk(os.path.join(home, directory)))
            logger.debug(__("Collect logs in directory {directory}", directory=current_directory))

            for file in files:
                if re.fullmatch(file_regex_pattern, file):
                    logger.debug(__("Collect log file {log}", log=file))
                    yield current_directory, file


def _insert_into_db_and_archive_logs(path_and_data: Iterator[Tuple[str, str, dict]]):
    """
    :param path_and_data: an iterator over directories, log file names of their data
    """
    logger.info("Start uploading log files")

    with influxdb.connect(**config["webike.influx"]) as client:
        for directory, filename, data in path_and_data:
            # noinspection PyBroadException
            try:
                if data is not None:
                    logger.debug(__("Upload file {file}", file=filename))
                    logger.debug(data)
                    client.write(data, {"db": config["webike.influx.database"]})
                    _archive_log(directory, filename)
                elif arguments["--strict"]:
                    _move_to_problem_folder(directory, filename)
            # try to import as many logs as possible, so just log any unexpected exceptions and keep going
            except:
                logger.error(__("Error with file {filename} in {directory}", filename=filename, directory=directory))
                _move_to_problem_folder(directory, filename)


def _archive_log(directory: str, filename: str):
    logger.debug(__("Archive file {file} in directory {dir}", file=filename, dir=directory))
    _move_to_subfolder(directory, filename, config["webike.archive"])


def _move_to_problem_folder(directory: str, filename: str):
    logger.warning(__("Move file {file} into problem folder in directory {dir}", file=filename, dir=directory))
    _move_to_subfolder(directory, filename, config["webike.problem"])


def _move_to_subfolder(directory: str, filename: str, subfolder: str):
    os.rename(os.path.join(directory, filename), os.path.join(directory, subfolder, filename))


config = load_config(module_locator.module_path())
logger = logging.getLogger()
arguments = docopt(__doc__)
import_data()
