class Logger:
    def __init__(self, filename:str, record:str=""):
        self._filename = self._get_log_filename(filename)
        self._record = record
        if record:
            self.write_log(record)

    @property
    def filename(self):
        return self._filename

    @property
    def record(self):
        return self.record

    def _get_log_filename(self, filename: str):
        return "/".join(filename.split("/")[:-1] + ["log.txt"])

    def write_log(self, record:str):
        with open(self.filename, "w") as f:
            f.write(record)