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


def app():
    """
    Application`s main entry point
    """
    print(__doc__)
    print("Running application\n")
    scrapper.run()
    print("\nExiting application")


if __name__ == '__main__':
    app()
