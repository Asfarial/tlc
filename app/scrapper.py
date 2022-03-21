"""
Module for scrapping a website
"""
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
from app.logger import Logger
from app.converter import Converter
from app.config import FORMATS, URL, DOWNLOAD_FOLDER


class Scrapper:
    """
    Scrapper for NY TLC Datasets
    """

    def __init__(self, url: str, years: int = 3):
        print("Initializing Scrapper")
        self._url = url
        self.soup = self.__get_soup_html()
        self.years = self.__get_years(years)

    @property
    def url(self):
        """
        Wrapper
        """
        return self._url

    @staticmethod
    def __format_parsed_years(last_year: str, first_year: str):
        """
        Formats parsed years,
        if years does not conform, gets standard values
        """
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

    def __get_years(self, years: int) -> list:
        """
        Returns years to be downloaded
        """
        years_parsed = self.soup.find_all("div", {"faq-questions"})
        last_year = years_parsed[0].text.strip()
        first_year = years_parsed[-1].text.strip()
        last_year, first_year = self.__format_parsed_years(
            last_year, first_year
        )
        sequence_start = (
            tmp if (tmp := last_year - years) >= first_year else first_year
        )
        return list(range(sequence_start, last_year))

    def __get_soup_html(self) -> BeautifulSoup:
        """
        Getting HTML of a URL as a BS4 object
        """
        print("Getting webpage HTML")
        response = requests.get(self.url)
        if response.status_code != 200:
            raise ConnectionError(
                f"\nCannot connect to specified url:"
                f" {self.url}\n"
                f"Status code: {response.status_code}"
            )
        return BeautifulSoup(response.text, "html.parser")

    def get_links(self) -> dict:
        """
        Parsing HTML for download links
        """
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

    def __create_downloads_folder(self) -> None:
        """
        Creates ./records
        """
        print("Checking downloads folder")
        if not os.path.exists(self.download_folder):
            os.mkdir(self.download_folder)

    @staticmethod
    def __make_filename(year: int, filename: str = "") -> str:
        """
        Makes filename for file to be downloaded
        """
        return os.path.join(Downloader.download_folder, str(year), filename)

    @staticmethod
    def __prepare_year_folder(year: int) -> None:
        """
        Creates year folder in ./records
        """
        path = Downloader.__make_filename(year)
        if not os.path.exists(path):
            os.mkdir(path)

    @staticmethod
    def __get_file_length_web(link: str) -> int:
        """
        Returns web file size
        for csv file downloading
        """
        req = requests.head(link)
        file_web_length = int(req.headers.get("content-length", 0))
        return file_web_length

    @staticmethod
    def __file_is_downloaded(filename: str,
                             file_web_length_bytes: int) -> bool:
        """
        Checks if a file is needed to be downloaded
        checks include csv, avro and parquet checking
        """
        if os.path.exists(filename):
            if os.path.getsize(filename) == file_web_length_bytes:
                return True

        logger = Logger(filename)
        if logger.log_exists:
            if (format_name := logger.get_file_format()) in FORMATS:
                if os.path.exists(
                        Converter.change_filename_extension(filename,
                                                            format_name)
                ):
                    return True
        return False

    @staticmethod
    def download(link: str,
                 filename: str,
                 file_web_length_bytes: int = None,
                 i: list = None
                 ) -> None:
        """
        The Downloading process itself
        """
        chunk_size = 655360
        if not file_web_length_bytes:
            file_web_length_bytes = Downloader.__get_file_length_web(link)
        file_web_length = file_web_length_bytes / 1024 / 1024
        start_pos_bytes = (
            os.path.getsize(filename) if os.path.exists(filename) else 0
        )
        resume_header = (
            {"Range": f"bytes={start_pos_bytes}-"} if start_pos_bytes else None
        )

        Downloader.__messages(
            link, file_web_length_bytes, i, start_pos_bytes=start_pos_bytes
        )

        with requests.get(link, stream=True, headers=resume_header) as req:
            req.raise_for_status()
            with open(filename, "ab" if start_pos_bytes else "wb") as file:
                progress = start_pos_bytes / 1024 / 1024
                for chunk in req.iter_content(chunk_size=chunk_size):
                    file.write(chunk)
                    progress += chunk_size / 1024 / 1024
                    progress = min(file_web_length, progress)
                    print(
                        f"\r{progress:.2f}MB/"
                        f"{file_web_length:.2f}MB",
                        end="",
                    )
        print("")
        Logger(filename).record_file_type("csv")

    @staticmethod
    def __get_links_count(links) -> int:
        """
        Method to get count of all links
        """
        length = 0
        for year in links:
            length += len(links[year])
        return length

    @staticmethod
    def __messages(
            link: str,
            file_web_length_bytes: int,
            i: list = None,
            start_pos_bytes: int = None,
            downloaded: bool = False,
    ) -> None:
        """
        Auto-Message printing, basis on args
        """
        file_web_length = file_web_length_bytes / 1024 / 1024

        if start_pos_bytes:
            message = (
                f"Resuming file download: {link},"
                f" Size: {start_pos_bytes / 1024 / 1024:.2f}MB/"
                f"{file_web_length:.2f} MB"
            )
        elif downloaded:
            message = f"File is already downloaded: {link}"
        else:
            message = (
                f"Downloading file: {link}," f" Size: {file_web_length:.2f} MB"
            )

        if i:
            print(f"[{i[0]}/{i[1]}] {message}")
        else:
            print(f"{message}")

    @staticmethod
    def download_files(links: dict, bypass=None) -> None:
        """
        Downloader methods Main wrapper
        """
        print("Downloading files...")
        if bypass:
            Downloader.__bypass_download()
        else:
            Downloader.__iterator(links)

    @staticmethod
    def __bypass_download() -> None:
        """
        Method to bypass downloading all datasets
        and to download 1 file
        for dev purpose only
        """
        print("Bypassing download of all files")
        link = (
            "https://s3.amazonaws.com/nyc-tlc/trip+data/"
            "green_tripdata_2021-01.csv"
        )
        Downloader.__prepare_year_folder(2021)
        Downloader.__download_pipeline(link, 2021, [1, 1])

    @staticmethod
    def __iterator(links: dict) -> None:
        """
        Iterator over links from Scrapper
        """
        i = [0, Downloader.__get_links_count(links)]
        for year in links:
            Downloader.__prepare_year_folder(year)
            for link in links[year]:
                i[0] += 1
                Downloader.__download_pipeline(link, year, i)

    @staticmethod
    def __download_pipeline(link: str, year: int, i: list = None) -> None:
        """
        High-level downloading method
        """
        filename = Downloader.__make_filename(year, link.split("/")[-1])
        file_web_length_bytes = Downloader.__get_file_length_web(link)
        if not Downloader.__file_is_downloaded(
                filename, file_web_length_bytes
        ):
            Downloader.download(link, filename, file_web_length_bytes, i)
        else:
            Downloader.__messages(
                link, file_web_length_bytes, i, downloaded=True
            )


def run(url: str = URL, bypass: bool = False) -> None:
    """
    Data pipeline for
    Scrapper
    """
    scrapper = Scrapper(url)
    links = scrapper.get_links()
    downloader = Downloader()
    downloader.download_files(links, bypass=bypass)
