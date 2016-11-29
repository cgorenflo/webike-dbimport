#!/usr/bin/python3.5

"""Imports all sensor data log files in the imei folders into the influxdb database

Usage:
  import_data.py [FILE] [-l | --legacy] [-s | --strict] [-d | --debug]

Optional Arguments:
  FILE          Imports a single file

Options:
  -h --help     Show this screen.
  -l --legacy   Use legacy parser for old data formats
  -s --strict   Moves logs that could not be imported into a problem folder.
                Files stay in place if this is not set
  -d --debug    Logs messages at DEBUG level

"""
import re
from concurrent.futures import ProcessPoolExecutor, wait

import iss4e.db.influxdb as influxdb
from docopt import docopt
# noinspection PyPep8Naming
from iss4e.util import BraceMessage as __
from iss4e.util.config import load_config

from iss4e.webike.db import module_locator
from iss4e.webike.db.csv_importers import *


def import_data():
    logger.info("Start log file import")

    if arguments["--legacy"]:
        logger.info("Using legacy formatter")
        csv_importer = LegacyImporter
    else:
        logger.info("Using formatter for well formed csv files")
        csv_importer = WellFormedCSVImporter

    if arguments["FILE"] is not None:
        directory_path = os.path.dirname(arguments["FILE"])
        if not os.path.isabs(directory_path):
            directory_path = os.path.join(os.getcwd(), directory_path)
        directory = Directory(os.path.basename(directory_path), directory_path)
        file = os.path.basename(arguments["FILE"])

        _execute_import(csv_importer(), directory, file)
    else:
        directories = _get_directories()
        with ProcessPoolExecutor(max_workers=14) as executor:
            futures = [executor.submit(_execute_import, csv_importer(), directory) for directory in directories]

            wait(futures)
    logger.info("Import complete")


def _execute_import(csv_importer: CSVImporter, directory: Directory, file: File = None) -> bool:
    file_regex_pattern = config["webike.logfile_regex"]
    if file is None:
        files = _get_files_in_directory(file_regex_pattern, directory)
    else:
        files = [file]
    logs = csv_importer.read_logs(directory, files)
    _insert_into_db_and_archive_logs(logs)

    return True


def _get_directories() -> Iterator[Directory]:
    """
    :returns an iterator over directories and log file names
    """
    logger.info("Start collecting log file directories")

    directory_regex_pattern = config["webike.imei_regex"]

    home, dirs, _ = next(os.walk(os.path.expanduser("~")))
    for directory in dirs:
        if re.fullmatch(directory_regex_pattern, directory):
            yield Directory(directory, os.path.join(home, directory))

    return []


def _get_files_in_directory(file_regex_pattern: str, directory: Directory) -> Iterator[File]:
    logger.debug(__("Collect logs in directory {directory}", directory=directory.name))
    _, _, files = next(os.walk(directory.abs_path))
    for file in files:
        if _filter_correct_files(file, file_regex_pattern):
            yield file

    return []


def _filter_correct_files(file: str, regex: str) -> bool:
    if re.fullmatch(regex, file):
        logger.debug(__("Collect log file {log}", log=file))
        return True
    return False


def _insert_into_db_and_archive_logs(path_and_data: Iterator[Tuple[Directory, File, Data]]):
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
            except KeyboardInterrupt:
                logger.error(__("Interrupted by user at file {filename} in {directory}", filename=filename,
                                directory=directory.name))
            except:
                logger.error(
                    __("Error with file {filename} in {directory}", filename=filename, directory=directory.name))
                _move_to_problem_folder(directory, filename)


def _archive_log(directory: Directory, filename: File):
    logger.debug(__("Archive file {file} in directory {dir}", file=filename, dir=directory.name))
    _move_to_subfolder(directory, filename, config["webike.archive"])


def _move_to_problem_folder(directory: Directory, filename: File):
    logger.warning(__("Move file {file} into problem folder in directory {dir}", file=filename, dir=directory.name))
    _move_to_subfolder(directory, filename, config["webike.problem"])


def _move_to_subfolder(directory: Directory, filename: str, subfolder: str):
    os.rename(os.path.join(directory.abs_path, filename), os.path.join(directory.abs_path, subfolder, filename))


arguments = docopt(__doc__)

config = load_config(module_locator.module_path())
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("iss4e.webike.db")
if arguments["--debug"]:
    logger.setLevel(logging.DEBUG)

import_data()
