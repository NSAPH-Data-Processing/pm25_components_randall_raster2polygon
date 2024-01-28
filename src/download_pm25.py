import os
import time
import hydra
import logging
import zipfile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
#from webdriver_manager.chrome import ChromeDriverManager
import shutil

logger = logging.getLogger(__name__)

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    """
    Download yearly V5 satellite PM2.5 data from Washington University's Atmospheric Composition Analysis Group.
    https://sites.wustl.edu/acag/datasets/surface-pm2-5/
    """

    # == url for download
    url = cfg.satellite_pm25[cfg.temporal_freq].url

    # == setup chrome driver
    # Expand the tilde to the user's home directory
    download_dir = "data/input/satellite_pm25"
    download_zip = f"{download_dir}/{cfg.satellite_pm25[cfg.temporal_freq].zipname}.zip"
    src_dir = f"{download_dir}/{cfg.satellite_pm25[cfg.temporal_freq].zipname}"
    dest_dir = f"{download_dir}/{cfg.temporal_freq}"

    # Set up Chrome options for headless mode and automatic downloads
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )

    # Setting up the Selenium WebDriver for Chrome using webdriver_manager
    #ChromeDriverManager().install()
    driver = webdriver.Chrome(options=chrome_options)
    logger.info("Chrome driver setup completed.")

    # == download file
    try:
        # Navigate to the website
        # Reload page (removes popup)
        driver.get(url)
        driver.refresh()
        logger.info("Webpage loaded.")

        # Wait for the button to be clickable
        download_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "button[aria-label='Download']")
            )
        )

        # Click the button
        download_button.click()
        logger.info("Downloading...")

        # Wait to make sure the file has downloaded
        while not os.path.exists(download_zip):
            time.sleep(5) #seconds
        logger.info("Download completed.")

        # Unzip all contents in the same folder
        with zipfile.ZipFile(download_zip, "r") as zip_ref:
            zip_ref.extractall(download_dir)

        # Move all files from the src_dir to dest_dir
        for file in os.listdir(src_dir):
            shutil.move(os.path.join(src_dir, file), dest_dir)

        # Remove the zip file and the empty folder
        os.remove(download_zip)
        os.rmdir(src_dir)

        logger.info("Unzipping completed.")

    except Exception as e:
        logger.error(e)

    finally:
        # Close the browser after completion or in case of an error
        driver.quit()
        logger.info("Download completed.")

if __name__ == "__main__":
    main()
