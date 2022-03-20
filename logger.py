import os
from config import FORMATS

class Logger:
    def __init__(self, filename:str, record:str=""):
        self._filename = self._get_filename_no_extension(filename)
        self._log_filename = self._get_log_filename(filename)
        self._record = record
        if record:
            self.append_log(record)

    @property
    def log_exists(self):
        return self._log_exists()

    @property
    def filename(self):
        return self._filename

    @property
    def log_filename(self):
        return self._log_filename

    @property
    def record(self):
        return self.record

    def _get_filename_no_extension(self, filename:str):
        return "." + filename.strip(".").split(".")[0]

    def _get_log_filename(self, filename: str):
        return "/".join(filename.split("/")[:-1] + ["log.txt"])

    def _log_exists(self):
        if os.path.exists(self.log_filename):
            return True
        return False

    def get_file_format(self):
        if self._log_exists():
            with open(self.log_filename, "r") as f:
                for line in f:
                    l_line = line.strip().split(": ")
                    if l_line[0] == self.filename:
                        return l_line[1]
        return None

    def record_file_type(self, filetype: str):
        filetype = filetype.strip().lower()
        if filetype not in FORMATS:
            raise TypeError(f"filetype shall be in {FORMATS}")
        if self._log_exists():
            with open(self.log_filename, "r") as f:
                content = f.readlines()
            file_record_found = False
            for i, line in enumerate(content):
                l_line = line.strip().split(": ")
                if l_line[0] == self.filename:
                    content[i] = l_line[0]+": "+filetype
                    self.write_log(content)
                    file_record_found = True
                    break
            if not file_record_found:
                record = self.filename + ": " + filetype
                self.append_log(record)
        else:
            content = self.filename + ": " + filetype
            self.write_log(content)

    def append_log(self, record:[str, list]):
        if not self._log_exists():
            self.write_log(record)
        else:
            with open(self.log_filename, "a") as f:
                if type(record) == list:
                    for line in record:
                        f.write(line + '\n')
                else:
                    f.write(record + '\n')

    def write_log(self, record:[str, list]):
        with open(self.log_filename, "w") as f:
            if type(record)==list:
                for line in record:
                    f.write(line+'\n')
            else:
                f.write(record + '\n')
