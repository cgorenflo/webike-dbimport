import os
from logging import Logger
from typing import Iterator
import re

from iss4e.webike.db.classes import Directory, File

# noinspection PyPep8Naming
from iss4e.util import BraceMessage as __


class FileSystemAccess(object):
    def __init__(self, logger: Logger):
        self._logger = logger

    def get_directories(self, directory_regex_pattern) -> Iterator[Directory]:
        """
        :returns an iterator over directories and log file names
        """
        self._logger.info("Start collecting log file directories")

        home, dirs, _ = next(os.walk(os.path.expanduser("~")))
        for directory in dirs:
            if re.fullmatch(directory_regex_pattern, directory):
                yield Directory(directory, os.path.join(home, directory))

        return []

    def get_files_in_directory(self, file_regex_pattern: str, directory: Directory) -> Iterator[File]:
        self._logger.debug(__("Collect logs in directory {directory}", directory=directory.name))
        _, _, files = next(os.walk(directory.abs_path))
        for file in files:
            if self._filter_correct_files(file, file_regex_pattern):
                yield file

        return []

    def _filter_correct_files(self, file: str, regex: str) -> bool:
        if re.fullmatch(regex, file):
            self._logger.debug(__("Collect log file {log}", log=file))
            return True
        return False
