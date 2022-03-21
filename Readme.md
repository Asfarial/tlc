This is a Test Task for MobiLab
-

The app will:
1) Parse NYC TLC website for datasets
2) Download at least last 3 years of data
2.1) Bypass - to download only 1 small file
3) Reformat CSV files into Avro and/or Parquet format
4) Queries Data for:
4.1) Average Trip Distance
4.2) Busiest Hours
4.3) Day of week with the lowest number of single rider trips

----
Installation:
-
Python: 3.10

Linux:

    python3.10 -m venv ./venv 
    source ./venv/bin/activate
    pip install -r requirements.txt
    python main.py
----
Code Structure:
-
Main.py

    Main entry point and SQL queries

Config.py
    
    Global Configurational Variables

Scrapper.py
    
    Scrapper and Downloader

Converter.py

    Converter

Logger.py
    
    Logger - to keep track of actual file format
----

Data Schema:
-
    ./tlc.GreenTaxi.avsc

----

Queries:
-
PySpark ORM and SQL

    main.queries

----
TODO:
-

 - Adjust for multifile and multiyear usage.<br>
 Possible decision: to use joins<br>
 Cannot resolve the problem: cannot download rest of the datasets, because of slow downloading speeding