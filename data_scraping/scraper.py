import logging
import os
import time
import zipfile

import requests
import toml
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from tqdm import tqdm

# Set up logger for the module.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

class CricsheetScraper:
    def __init__(
            self,
            config_path=None,
            download_folder=None,
            extract_folder=None,
            driver_path=None,
    ):
        """
        Initializes the scraper with configuration, download and extraction folders,
        and an optional driver path.
        Data folders are created relative to the project root.
        """
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        logger.info(f"Project root determined as: {project_root}")

        if not config_path:
            config_path = os.path.join(project_root, "config.toml")
        if not download_folder:
            download_folder = os.path.join(project_root, "data", "downloads")
        if not extract_folder:
            extract_folder = os.path.join(project_root, "data", "extracted")

        data_folder = os.path.join(project_root, "data")
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
            logger.info(f"Created top-level data folder: {data_folder}")

        self.download_folder = download_folder
        self.extract_folder = extract_folder

        for folder in [self.download_folder, self.extract_folder]:
            if not os.path.exists(folder):
                os.makedirs(folder)
                logger.info(f"Created folder: {folder}")

        try:
            self.config = toml.load(config_path)
            logger.info(f"Configuration loaded from {config_path}")
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_path}: {e}")
            raise

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        try:
            if driver_path:
                self.driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)
            else:
                self.driver = webdriver.Chrome(options=chrome_options)
            logger.info("Chrome driver initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            raise

    def scrape_matches(self):
        """
        Navigates to the Cricsheet matches page, extracts all JSON and ZIP file links
        (only those whose anchor text fully matches "JSON"), downloads them (if not already downloaded),
        and then unzips only the specified files (if not already extracted).
        """
        url = self.config["urls"]["cricsheet_matches"]
        self.driver.get(url)
        logger.info(f"Navigated to {url}")
        time.sleep(3)

        links = self.driver.find_elements(By.TAG_NAME, "a")
        download_links = {}

        for link in links:
            if link.text.strip() == "JSON":
                href = link.get_attribute("href")
                if href and (href.endswith(".zip") or href.endswith(".json")):
                    file_name = href.split("/")[-1]
                    download_links[file_name] = href

        logger.info(f"Found download links: {download_links}")

        for file_name, link in download_links.items():
            if file_name in [
                self.config["downloads"]["tests_json"],
                self.config["downloads"]["odis_json"],
                self.config["downloads"]["t20s_json"],
            ]:
                local_file = self.download_file(link)
                self.extract_zip_file(local_file)

    def download_file(self, url):
        """
        Downloads a file from the given URL and saves it in the designated download folder.
        Displays a progress bar using tqdm.
        Returns the local file path.
        """
        local_filename = os.path.join(self.download_folder, url.split("/")[-1])
        if os.path.exists(local_filename):
            logger.info(f"File already exists: {local_filename}. Skipping download.")
            return local_filename

        logger.info(f"Downloading {url} to {local_filename}")
        try:
            with requests.get(url, stream=True, timeout=10) as response:
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                chunk_size = 8192
                progress_bar = tqdm(total=total_size, unit="iB", unit_scale=True)
                with open(local_filename, "wb") as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            progress_bar.update(len(chunk))
                progress_bar.close()
            logger.info(f"Downloaded {local_filename}")
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            raise

        return local_filename

    def extract_zip_file(self, zip_path):
        """
        Extracts the contents of a zip file into a dedicated subfolder within the extraction folder.
        Skips extraction if the subfolder already exists.
        """
        base_name = os.path.splitext(os.path.basename(zip_path))[0]
        extraction_path = os.path.join(self.extract_folder, base_name)

        if os.path.exists(extraction_path):
            logger.info(f"Extraction folder already exists for {zip_path} at {extraction_path}. Skipping extraction.")
            return

        logger.info(f"Extracting {zip_path} into {extraction_path}")
        os.makedirs(extraction_path, exist_ok=True)
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extraction_path)
            logger.info(f"Extraction complete for {zip_path}")
        except zipfile.BadZipFile:
            logger.error(f"Error: {zip_path} is not a valid zip file.")

    def close(self):
        """
        Closes the Selenium webdriver.
        """
        self.driver.quit()
        logger.info("Chrome driver closed.")

if __name__ == "__main__":
    scraper = CricsheetScraper()
    try:
        scraper.scrape_matches()
    except Exception as e:
        logger.error(f"An error occurred during scraping: {e}")
    finally:
        scraper.close()
