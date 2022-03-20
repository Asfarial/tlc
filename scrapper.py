"""
Module for scrapping a website
"""
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
from logger import Logger
from converter import Converter
from config import FORMATS
from config import URL, DOWNLOAD_FOLDER



class Scrapper:
    def __init__(self, url: str, years: int = 3):
        print("Initializing Scrapper")
        self.url = url
        self.soup = self.__get_soup_html()
        self.years = self.__get_years(years)

    @staticmethod
    def __format_parsed_years(last_year: str, first_year: str):
        if last_year.isnumeric():
            last_year = int(last_year)
        else:
            last_year = datetime.now().year
        last_year += 1

        if first_year.isnumeric():
            first_year = int(first_year)
        else:
            first_year = 2009
        return last_year, first_year

    def __get_years(self, years: int):
        years_parsed = self.soup.find_all("div", {"faq-questions"})
        last_year = years_parsed[0].text.strip()
        first_year = years_parsed[-1].text.strip()
        last_year, first_year = self.__format_parsed_years(
            last_year, first_year
        )
        sequence_start = (
            tmp if (tmp := last_year - years) >= first_year else first_year
        )
        return [year for year in range(sequence_start, last_year)]

    def __get_soup_html(self):
        print("Getting webpage HTML")
        response = requests.get(self.url)
        if response.status_code != 200:
            raise ConnectionError(
                f"\nCannot connect to specified url:"
                f" {self.url}\n"
                f"Status code: {response.status_code}"
            )
        return BeautifulSoup(response.text, "html.parser")

    def get_links(self):
        print("Parsing HTML for download links")
        links = {}
        for year in self.years:
            div_year = self.soup.find("div", {"id": f"faq{year}"})
            links[year] = [
                a["href"]
                for a in div_year.find_all(
                    "a", {"title": "Yellow Taxi Trip Records"}
                )
            ]
            links[year] += [
                a["href"]
                for a in div_year.find_all(
                    "a", {"title": "Green Taxi Trip Records"}
                )
            ]
        return links


class Downloader:
    """
    DOWNLOADER
    yet has a way to improve using Celery
    to have trackable downloads in background
    """
    download_folder = DOWNLOAD_FOLDER

    def __init__(self, download_folder: str = download_folder):
        print("Initializing Downloader")
        self.download_folder = download_folder
        self.__create_downloads_folder()

    def __create_downloads_folder(self):
        print("Checking downloads folder")
        if not os.path.exists(self.download_folder):
            os.mkdir(self.download_folder)

    @staticmethod
    def __make_filename(year: int, filename: str = ""):
        return os.path.join(Downloader.download_folder, str(year), filename)

    @staticmethod
    def __prepare_year_folder(year: int):
        path = Downloader.__make_filename(year)
        if not os.path.exists(path):
            os.mkdir(path)

    @staticmethod
    def __get_file_length_web(link: str):
        r = requests.head(link)
        file_web_length = int(r.headers.get('content-length', 0))
        return file_web_length
    @staticmethod
    def __file_is_downloaded(filename: str, file_web_length_bytes: int):
        if os.path.exists(filename):
            if os.path.getsize(filename) == file_web_length_bytes:
                return True

        logger = Logger(filename)
        if logger.log_exists:
            if (format_name:=logger.get_file_format()) in FORMATS:
                if os.path.exists(Converter.change_filename_extension(filename, format_name)):
                    return True
        return False

    @staticmethod
    def __download(link: str, filename: str, file_web_length_bytes: int, i: list = None):
        chunk_size = 655360
        file_web_length = file_web_length_bytes / 1024 / 1024
        start_pos_bytes = os.path.getsize(filename) if os.path.exists(filename) else 0
        resume_header = ({'Range': f'bytes={start_pos_bytes}-'}
                         if start_pos_bytes else None)

        Downloader.__messages(link, file_web_length_bytes, i, start_pos_bytes=start_pos_bytes)

        with requests.get(link, stream=True, headers=resume_header) as r:
            r.raise_for_status()
            with open(filename, "ab" if start_pos_bytes else "wb") as f:
                progress = start_pos_bytes / 1024 / 1024
                for chunk in r.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
                    progress += chunk_size / 1024 / 1024
                    print(f"\r{file_web_length if progress>file_web_length else progress:.2f}MB/{file_web_length:.2f}MB", end='')
        print("")
        Logger(filename).record_file_type("csv")

    @staticmethod
    def __get_links_count(links):
        length = 0
        for year in links:
            length += len(links[year])
        return length

    @staticmethod
    def __bypass_download():
        print("Bypassing download of all files")
        link = "https://s3.amazonaws.com/nyc-tlc/trip+data/" \
               "green_tripdata_2021-01.csv"
        Downloader.__prepare_year_folder(2021)
        Downloader.__download_pipeline(link, 2021, [1, 1])

    @staticmethod
    def __iterator(links: dict):
        i = [0, Downloader.__get_links_count(links)]
        for year in links:
            Downloader.__prepare_year_folder(year)
            for link in links[year]:
                i[0] += 1
                Downloader.__download_pipeline(link, year, i)

    @staticmethod
    def __messages(link: str,
                   file_web_length_bytes: int,
                   i: list = None,
                   start_pos_bytes: int = None,
                   downloaded: bool = False):

        file_web_length = file_web_length_bytes / 1024 / 1024

        if start_pos_bytes:
            message = f"Resuming file download: {link}," \
                      f" Size: {start_pos_bytes/ 1024 / 1024:.2f}MB/{file_web_length:.2f} MB"
        elif downloaded:
            message = f"File is already downloaded: {link}"
        else:
            message = f"Downloading file: {link}," \
                  f" Size: {file_web_length:.2f} MB"

        if i:
            print(f"[{i[0]}/{i[1]}] {message}")
        else:
            print(f"{message}")

    @staticmethod
    def __download_pipeline(link: str, year: int, i: list = None):
        filename = Downloader.__make_filename(year, link.split("/")[-1])
        file_web_length_bytes = Downloader.__get_file_length_web(link)
        if not Downloader.__file_is_downloaded(filename, file_web_length_bytes):
            Downloader.__download(link, filename, file_web_length_bytes, i)
        else:
            Downloader.__messages(link, file_web_length_bytes, i, downloaded=True)

    @staticmethod
    def download_files(links: dict, bypass=None):
        print("Downloading files...")
        if bypass:
            Downloader.__bypass_download()
        else:
            Downloader.__iterator(links)


def run(url: str = URL, bypass: bool = False):
    scrapper = Scrapper(url)
    links = scrapper.get_links()
    downloader = Downloader()
    downloader.download_files(links, bypass=bypass)
