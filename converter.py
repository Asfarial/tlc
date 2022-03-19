"""
Converter
"""
import csv
import os.path

import pandas as pd
from fastavro import writer, parse_schema
from fastavro.schema import load_schema


class Converter:
    avro_schema = {
        "doc": "TLC",
        "name": "Green Taxi",
        "namespace": "tlc",
        "type": "record",
        "fields": [
            {"name": "VendorID", "type": "int"},
            {
                "name": "lpep_pickup_datetime",
                "type": {"type": "string", "logicalType": "time-millis"},
            },
            {
                "name": "lpep_dropoff_datetime",
                "type": {"type": "string", "logicalType": "time-millis"},
            },
            {"name": "store_and_fwd_flag", "type": "string"},
            {"name": "RatecodeID", "type": "int"},
            {"name": "PULocationID", "type": "int"},
            {"name": "DOLocationID", "type": "int"},
            {"name": "passenger_count", "type": "int"},
            {"name": "trip_distance", "type": "float"},
            {"name": "fare_amount", "type": "float"},
            {"name": "extra", "type": "float"},
            {"name": "mta_tax", "type": "float"},
            {"name": "tip_amount", "type": "float"},
            {"name": "tolls_amount", "type": "float"},
            {"name": "ehail_fee", "type": "float"},
            {"name": "improvement_surcharge", "type": "float"},
            {"name": "total_amount", "type": "float"},
            {"name": "payment_type", "type": "int"},
            {"name": "trip_type", "type": "int"},
            {"name": "congestion_surcharge", "type": "float"},
        ],
    }

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

    def to_avro_orig(self, filename: str):
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
        with open("/".join(filename_avro.split("/")[:-1]+["log.txt"]), "w") as f:
            f.write("avro")
