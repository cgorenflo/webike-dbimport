#!/usr/bin/python3.5

"""Imports all sensor data log files in the imei folders into the influxdb database

Usage:
  import_data.py [FILE] [--version=VERSION_NUMBER] [-s | --strict] [-a | --archive] [-d | --debug]

Optional Arguments:
  FILE                      Imports a single file

Options:
  -h --help                 Show this screen.
  --version=VERSION_NUMBER  Imports data log files using a parser for the specified format version [default: 3]
  -s --strict               Moves logs that could not be imported into a problem folder.
                            Files stay in place if this is not set
  -d --debug                Logs messages at DEBUG level
  -a --archive              Move all log files from the main folders into the archives

"""
from concurrent.futures import ProcessPoolExecutor, wait
from multiprocessing.managers import SyncManager

import iss4e.db.influxdb as influxdb
from docopt import docopt
# noinspection PyPep8Naming
from iss4e.util import BraceMessage as __, progress, async_progress
from iss4e.util.config import load_config
from multiprocessing import Queue

from iss4e.webike.db import module_locator
from iss4e.webike.db.csv_parser import *
from iss4e.webike.db.file_system_access import FileSystemAccess


def import_data():
    logger.info("Start log file import")

    parsers = [V1Parser, V2Parser, V3Parser]
    logger.info(__("Using parser version {version}", version=arguments["--version"]))
    csv_parser = parsers[int(arguments["--version"]) - 1]

    if arguments["FILE"] is not None:
        file_path = arguments["FILE"]
        if not os.path.isabs(file_path):
            file_path = os.path.join(os.getcwd(), file_path)

        directory_path = os.path.dirname(file_path)
        directory = Directory(os.path.basename(directory_path), directory_path)
        file = os.path.basename(file_path)

        logger.debug(__("directory: {dir}, file:{file}", dir=directory, file=file))

        _execute_import(csv_parser(), directory, file)
    else:

        directories = FileSystemAccess(logger).get_directories(config["webike.imei_regex"])
        manager = SyncManager()
        manager.start()
        queue = manager.Queue()
        with ProcessPoolExecutor(max_workers=14) as executor:
            futures = [executor.submit(_execute_import, csv_parser(), directory, queue) for directory in
                       directories]

            async_progress(futures, queue)
    logger.info("Import complete")


def _execute_import(csv_importer: CSVParser, directory: Directory, file: File = None, queue: Queue = None) -> bool:
    file_regex_pattern = config["webike.logfile_regex"]
    if file is None:
        files = FileSystemAccess(logger).get_files_in_directory(file_regex_pattern, directory)
    else:
        files = [file]
    logs = csv_importer.read_logs(directory, files)
    try:
        _insert_into_db_and_archive_logs(logs, queue)
    except KeyboardInterrupt:
        raise
    except:
        logger.exception("Unexpected Exception")

    return True


def _insert_into_db_and_archive_logs(path_and_data: Iterator[Tuple[Directory, File, Data]], queue: Queue = None):
    """
    :param path_and_data: an iterator over directories, log file names of their data
    """

    if arguments["--archive"]:
        logger.info("Start archiving all files")
    else:
        logger.info("Start uploading log files")

    with influxdb.connect(**config["webike.influx"]) as client:
        for directory, filename, data in progress(path_and_data, delay=10, remote=queue):
            # noinspection PyBroadException
            try:
                if arguments["--archive"]:
                    _archive_log(directory, filename)
                elif data is not None:
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
                raise
            except Exception as exception:
                logger.exception(
                    __("Error with file {filename} in {directory}:", filename=filename, directory=directory.name))
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
logger = logging.getLogger("iss4e.webike.db")
if arguments["--debug"]:
    logger.setLevel(logging.DEBUG)

import_data()
