"""
Converter
"""
import os
from os import listdir
from os.path import isfile
from scrapper import DOWNLOAD_FOLDER
from logger import Logger
import pandas as pd
from fastavro import writer
from fastavro.schema import load_schema


class Converter:

    def __init__(self):
        print("Initializing Converter")

    @staticmethod
    def conv_int(val: str):
        if not val:
            return 0
        return int(val)

    @staticmethod
    def conv_str(val: str):
        if not val:
            return ""
        return val

    @staticmethod
    def conv_float(val: str):
        if not val:
            return 0
        return float(val)

    def to_avro(self, filename: str):
        chunksize = 1000000
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

        def _reset_file(path):
            file = open(path, "wb")
            file.close()

        if not os.path.exists(filename):
            raise FileNotFoundError(filename)

        filename_avro = "." + filename.strip(".").split(".")[0] + ".avro"
        parsed_schema = load_schema("tlc.GreenTaxi.avsc")
        _reset_file(filename_avro)
        with pd.read_csv(
            filename, converters=converters, chunksize=chunksize
        ) as df:
            for chunk in df:
                records = chunk.to_dict("records")
                with open(filename_avro, "ab+") as out:
                    writer(out, parsed_schema, records, codec="snappy")
        Logger(filename, "avro")

    def read_avro(self, filename: str):
        import fastavro
        import pandas as pd

        avro_records = []
        with open(filename, "rb") as f:
            avro_reader = fastavro.reader(f)
            for record in avro_reader:
                avro_records.append(record)
        df_avro = pd.DataFrame(avro_records)
        print(df_avro.head())

    def convert_all(self):
        all_files = self._get_all_csv()
        if not (l:=len(all_files)):
            print("No files to convert")
            return
        print("Converting csv files to avro")
        for i, file in enumerate(all_files, 1):
            print(f"{i}/{l}) {file}")
            self.to_avro(file)
            print(f"Successfully converted\n"
                  f"Deleting csv file")
            os.remove(file)

    def _get_all_csv(self, download_folder:str = DOWNLOAD_FOLDER):
        print("Collecting all csv filenames")
        def make_filename(base,file):
            return "/".join([base, file])
        folders = listdir(download_folder)
        folders = ["".join([download_folder, f]) for f in folders]
        return [full_file for folder in folders for file in listdir(folder) if file.split('.')[-1]=='csv' if isfile((full_file := make_filename(folder, file)))]

def run():
    converter = Converter()
    converter.convert_all()
