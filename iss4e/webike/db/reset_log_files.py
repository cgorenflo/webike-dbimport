#!/usr/bin/python3.5

import os

import logging
from docopt import docopt
from iss4e.util.config import load_config

from iss4e.webike.db import import_data
from iss4e.webike.db import module_locator
from iss4e.util import BraceMessage as __


def reset():
    directories = import_data.get_directories()
    for directory in directories:
        logger.info(__("Moving files from archive folder in {dir} back to main folder.", dir=directory))
        archive = os.path.join(directory, config["webike.archive"])
        _move_to_parent(archive)
        logger.info("Moving files from problem folder in {dir} back to main folder.", dir=directory)
        problem = os.path.join(directory, config["webike.problem"])
        _move_to_parent(problem)


def _move_to_parent(directory):
    files = os.listdir(directory)
    for file in files:
        full_file_name = os.path.join(directory, file)
        if (os.path.isfile(full_file_name)):
            os.rename(full_file_name, os.path.join(directory, os.path.pardir))


arguments = docopt(__doc__)

config = load_config(module_locator.module_path())
logger = logging.getLogger("iss4e.webike.db.reset")

reset()
