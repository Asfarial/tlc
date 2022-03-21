"""
Converter
"""
import os
from os import listdir
from os.path import isfile
import pandas as pd
import pyarrow.parquet as pq
from fastavro import writer, reader
from fastavro.schema import load_schema
from fastparquet import write
from app.config import DOWNLOAD_FOLDER
from app.logger import Logger


class Converter:
    """
    Converter csv, avro, parquet
    """
    chunksize = 1000000

    @property
    def converters(self):
        """
        Wrapper for default cls properties
        """
        converters = {
            "VendorID": self.conv_int,
            "lpep_pickup_datetime": self.conv_str,
            "lpep_dropoff_datetime": self.conv_str,
            "store_and_fwd_flag": self.conv_str,
            "RatecodeID": self.conv_int,
            "PULocationID": self.conv_int,
            "DOLocationID": self.conv_int,
            "passenger_count": self.conv_int,
            "trip_distance": self.conv_float,
            "fare_amount": self.conv_float,
            "extra": self.conv_float,
            "mta_tax": self.conv_float,
            "tip_amount": self.conv_float,
            "tolls_amount": self.conv_float,
            "ehail_fee": self.conv_float,
            "improvement_surcharge": self.conv_float,
            "total_amount": self.conv_float,
            "payment_type": self.conv_int,
            "trip_type": self.conv_int,
            "congestion_surcharge": self.conv_float,
        }
        return converters

    def __init__(self):
        print("Initializing Converter")

    @staticmethod
    def conv_int(val: str) -> int:
        """
        Converter
        field normalizer
        """
        if not val:
            return 0
        return int(val)

    @staticmethod
    def conv_str(val: str) -> str:
        """
        Converter
        field normalizer
        """
        if not val:
            return ""
        return val

    @staticmethod
    def conv_float(val: str) -> float:
        """
        Converter
        field normalizer
        """
        if not val:
            return 0
        return float(val)

    def csv_to_avro(self, filename: str) -> str:
        """
        Convert csv to avro
        """
        self._check_file_exists(filename)
        filename_avro = "." + filename.strip(".").split(".")[0] + ".avro"
        parsed_schema = load_schema("../tlc.GreenTaxi.avsc")
        self._reset_file(filename_avro)
        with pd.read_csv(
            filename, converters=self.converters, chunksize=self.chunksize
        ) as d_frame:
            for chunk in d_frame:
                records = chunk.to_dict("records")
                with open(filename_avro, "ab+") as out:
                    writer(out, parsed_schema, records, codec="snappy")
        Logger(filename).record_file_type("avro")
        os.remove(filename)
        return filename_avro

    @staticmethod
    def _check_file_exists(filename) -> None:
        """
        File existence test
        """
        if not os.path.exists(filename):
            raise FileNotFoundError(filename)

    @staticmethod
    def _reset_file(path) -> None:
        """
        Reset/create avro file
        Because in converting to avro
        appending mode is used
        """
        with open(path, "wb"):
            pass

    def csv_to_parquet(self, filename: str) -> str:
        """
        Converts csv to parquet
        """
        self._check_file_exists(filename)
        filename_parquet = "." + filename.strip(".").split(".")[0] + ".parquet"
        self._reset_file(filename_parquet)
        d_frame = pd.read_csv(filename, converters=self.converters)
        d_frame.to_parquet(filename_parquet)
        Logger(filename).record_file_type("parquet")
        os.remove(filename)
        return filename_parquet

    def avro_to_parquet(self, filename: str) -> str:
        """
        Converts avro to parquet
        """
        d_frame = self.read_avro(filename)
        parquet_filename = self.change_filename_extension(filename, "parquet")
        write(parquet_filename, d_frame, compression="snappy")
        Logger(filename).record_file_type("parquet")
        os.remove(filename)
        return parquet_filename

    @staticmethod
    def read_parquet(filename: str) -> pd.DataFrame:
        """
        Read Parquet
        """
        table = pq.read_table(filename)
        d_frame = table.to_pandas()
        return d_frame

    @staticmethod
    def read_avro(filename: str) -> pd.DataFrame:
        """
        Read Avro
        """
        with open(filename, "rb") as file:
            avro_reader = reader(file)
            avro_records = list(avro_reader)
            df_avro = pd.DataFrame(avro_records)
            return df_avro

    def convert_all(self) -> None:
        """
        Pipeline
        converts all downloaded to parquet
        """
        all_files = self._get_all_csv()
        if not (len_files := len(all_files)):
            print("No files to convert")
            return
        print("Converting files")
        for i, file in enumerate(all_files, 1):
            print(f"{i}/{len_files}) {file}")
            self.csv_to_parquet(file)

    @staticmethod
    def _get_all_csv(download_folder: str = DOWNLOAD_FOLDER) -> list:
        """
        Collecting all csv filenames[paths]
        """
        print("Collecting all csv filenames")

        def make_filename(base, file) -> str:
            """
            Inner Scope
            Returns file path
            """
            return "/".join([base, file])

        folders = listdir(download_folder)
        folders = ["".join([download_folder, f]) for f in folders]
        return [
            full_file
            for folder in folders
            for file in listdir(folder)
            if file.split(".")[-1] == "csv"
            if isfile((full_file := make_filename(folder, file)))
        ]

    @staticmethod
    def change_filename_extension(filename: str, extension: str) -> str:
        """
        Changes filename extension
        """
        return (
            "."
            + filename.strip(".").split(".")[0]
            + "."
            + extension.strip(".")
        )


def run() -> None:
    """
    Data pipeline for
    Converter
    """
    converter = Converter()
    converter.convert_all()
