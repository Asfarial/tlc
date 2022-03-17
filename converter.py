"""
Converter
"""
import pandas as pd
from fastavro import writer, parse_schema


class Converter:
    avro_schema = {
        'doc': 'TLC',
        'name': 'Green Taxi',
        'namespace': 'tlc',
        'type': 'record',
        'fields': [
            {'name': 'VendorID', 'type': 'int'},
            {'name': 'lpep_pickup_datetime', 'type': {'type': 'string', 'logicalType': 'time-millis'}},
            {'name': 'lpep_dropoff_datetime', 'type': {'type': 'string', 'logicalType': 'time-millis'}},
            {'name': 'store_and_fwd_flag', 'type': 'string'},
            {'name': 'RatecodeID', 'type': 'int'},
            {'name': 'PULocationID', 'type': 'int'},
            {'name': 'DOLocationID', 'type': 'int'},
            {'name': 'passenger_count', 'type': 'int'},
            {'name': 'trip_distance', 'type': 'float'},
            {'name': 'fare_amount', 'type': 'float'},
            {'name': 'extra', 'type': 'float'},
            {'name': 'mta_tax', 'type': 'float'},
            {'name': 'tip_amount', 'type': 'float'},
            {'name': 'tolls_amount', 'type': 'float'},
            {'name': 'ehail_fee', 'type': 'float'},
            {'name': 'improvement_surcharge', 'type': 'float'},
            {'name': 'total_amount', 'type': 'float'},
            {'name': 'payment_type', 'type': 'int'},
            {'name': 'trip_type', 'type': 'int'},
            {'name': 'congestion_surcharge', 'type': 'float'}
        ]
    }

    @staticmethod
    def conv_int(val:str):
        if not val:
            return -1
        return int(val)

    @staticmethod
    def conv_str(val:str):
        if not val:
            return ''
        return val

    @staticmethod
    def conv_float(val:str):
        if not val:
            return float(-404)
        return float(val)


    def convert(self, filename:str, format:str):
        df = pd.read_csv(filename, converters={
            'VendorID': self.conv_int,
            'lpep_pickup_datetime': self.conv_str,
            'lpep_dropoff_datetime': self.conv_str,
            'store_and_fwd_flag': self.conv_str,
            'RatecodeID': self.conv_int,
            'PULocationID': self.conv_int,
            'DOLocationID': self.conv_int,
            'passenger_count': self.conv_int,
            'trip_distance': self.conv_float,
            'fare_amount': self.conv_float,
            'extra': self.conv_float,
            'mta_tax': self.conv_float,
            'tip_amount': self.conv_float,
            'tolls_amount': self.conv_float,
            'ehail_fee': self.conv_float,
            'improvement_surcharge': self.conv_float,
            'total_amount': self.conv_float,
            'payment_type': self.conv_int,
            'trip_type': self.conv_int,
            'congestion_surcharge': self.conv_float})
        if format == 'avro':
            self.to_avro(df, filename)

    def to_avro(self, df, filename:str):
        def avro_filename(name:str):
            return '.'+name.strip('.').split('.')[0]+'.avro'
        parsed_schema = parse_schema(self.avro_schema)
        records = df.to_dict('records')
        with open(avro_filename(filename), 'wb') as out:
            writer(out, parsed_schema, records)