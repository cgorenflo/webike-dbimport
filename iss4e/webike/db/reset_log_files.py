#!/usr/bin/python3.5

"""Moves all previously processed log files back into the main folder

Usage:
  reset_log_files.py [-a | --archive][-p | --problem]

Options:
  -h --help     Show this screen.
  -a --archive  Only move back files from the archive folder
  -p --problem  Only move back files from the problem folder

"""

import os

import logging

from docopt import docopt
from iss4e.util.config import load_config

from iss4e.webike.db import module_locator
# noinspection PyPep8Naming
from iss4e.util import BraceMessage as __

from iss4e.webike.db.file_system_access import FileSystemAccess


def reset():
    logger.info(__("Getting all necessary directories"))
    file_system_access = FileSystemAccess(logger)
    directories = file_system_access.get_directories(config["webike.imei_regex"])
    default_behaviour = (not arguments["--archive"] and not arguments["--problem"])
    for directory in directories:
        if arguments["--archive"] or default_behaviour:
            logger.info(__("Moving files from archive folder in {dir} back to main folder.", dir=directory.name))
            archive = os.path.join(directory.abs_path, config["webike.archive"])
            _move_to_parent(archive)
        if arguments["--problem"] or default_behaviour:
            logger.info(__("Moving files from problem folder in {dir} back to main folder.", dir=directory.name))
            problem = os.path.join(directory.abs_path, config["webike.problem"])
            _move_to_parent(problem)


def _move_to_parent(directory: str):
    files = os.listdir(directory)
    for file in files:
        full_file_name = os.path.join(directory, file)
        if os.path.isfile(full_file_name):
            os.rename(full_file_name, os.path.join(os.path.join(directory, os.path.pardir), file))


arguments = docopt(__doc__)

config = load_config(module_locator.module_path())
logger = logging.getLogger("iss4e.webike.db.reset")

reset()
