"""
Logger Module
"""
import os
from app.config import FORMATS


class Logger:
    """
    Logger to keep track of actual file format
    """

    def __init__(self, filename: str, record: str = ""):
        self._filename = self._get_filename_no_extension(filename)
        self._log_filename = self._get_log_filename(filename)
        self._record = record
        if record:
            self.append_log(record)

    @property
    def log_exists(self) -> bool:
        """
        Wrapper
        """
        return self._log_exists()

    @property
    def filename(self) -> str:
        """
        Wrapper
        """
        return self._filename

    @property
    def log_filename(self) -> str:
        """
        Wrapper
        """
        return self._log_filename

    @property
    def record(self) -> str:
        """
        Wrapper
        """
        return self.record

    @staticmethod
    def _get_filename_no_extension(filename: str) -> str:
        """
        Makes filename without extension for a record
        """
        return "." + filename.strip(".").split(".")[0]

    @staticmethod
    def _get_log_filename(filename: str) -> str:
        """
        Get filename for log file
        """
        return "/".join(filename.split("/")[:-1] + ["log.txt"])

    def _log_exists(self) -> bool:
        """
        Checks if a log exists
        Wrapper
        """
        if os.path.exists(self.log_filename):
            return True
        return False

    def get_file_format(self) -> str:
        """
        Gets storage format from a record
        """
        if self._log_exists():
            with open(self.log_filename, "r", encoding='utf-8') as file:
                for line in file:
                    l_line = line.strip().split(": ")
                    if l_line[0] == self.filename:
                        return l_line[1]
        return ""

    def record_file_type(self, filetype: str) -> None:
        """
        Record stored file type to log
        """
        filetype = filetype.strip().lower()
        if filetype not in FORMATS:
            raise TypeError(f"filetype shall be in {FORMATS}")
        if self._log_exists():
            with open(self.log_filename, "r", encoding='utf-') as file:
                content = file.readlines()
            file_record_found = False
            for i, line in enumerate(content):
                l_line = line.strip().split(": ")
                if l_line[0] == self.filename:
                    content[i] = l_line[0] + ": " + filetype
                    self.write_log(content)
                    file_record_found = True
                    break
            if not file_record_found:
                record = self.filename + ": " + filetype
                self.append_log(record)
        else:
            content = self.filename + ": " + filetype
            self.write_log(content)

    def append_log(self, record: [str, list]) -> None:
        """
        Appending log
        """
        if not self._log_exists():
            self.write_log(record)
        else:
            with open(self.log_filename, "a", encoding='utf-8') as file:
                if isinstance(record, list):
                    for line in record:
                        file.write(line + "\n")
                else:
                    file.write(record + "\n")

    def write_log(self, record: [str, list]) -> None:
        """
        Overwriting Log
        """
        with open(self.log_filename, "w", encoding='utf-8') as file:
            if isinstance(record, list):
                for line in record:
                    file.write(line + "\n")
            else:
                file.write(record + "\n")
