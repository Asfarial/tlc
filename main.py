"""
This is Test Task for MobiLab

The app will:
1) Parse NYC TLC website for datasets
2) Reformat CSV files in Avro and Parquet format
3) Upload Data into PostgreSQL
4) Have interface for 3 standard queries
5) Have interface for >= 3 years of NYC TLC datasets download
"""
import scrapper
from converter import Converter


def read_avro(filename: str):
    import fastavro
    import pandas as pd

    avro_records = []
    with open(filename, "rb") as f:
        avro_reader = fastavro.reader(f)
        for record in avro_reader:
            avro_records.append(record)
    df_avro = pd.DataFrame(avro_records)
    print(df_avro.head())


def app():
    """
    Application`s main entry point
    """
    print(__doc__)
    print("Running application\n")
    scrapper.run()
    #converter = Converter()
    #converter.convert("./records/2021/green_tripdata_2021-01.csv", "avro")
    # read_avro('./records/2021/green_tripdata_2021-01.avro')
    print("\nExiting application")


if __name__ == "__main__":
    app()
