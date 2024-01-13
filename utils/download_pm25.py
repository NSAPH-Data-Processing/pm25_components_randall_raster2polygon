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
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    """
    Download yearly V5 satellite PM2.5 data from Washington University's Atmospheric Composition Analysis Group.
    https://sites.wustl.edu/acag/datasets/surface-pm2-5/
    """

    # == url for download
    url = cfg.satellite_pm25.url

    # == setup chrome driver
    # Expand the tilde to the user's home directory
    target_dir = os.path.expanduser(cfg.target_dir)
    target_file = f"{target_dir}/{cfg.satellite_pm25.zipname}.zip"

    # Set up Chrome options for headless mode and automatic downloads
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": target_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )

    # Setting up the Selenium WebDriver for Chrome using webdriver_manager
    ChromeDriverManager().install()
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
        while not os.path.exists(target_file):
            time.sleep(cfg.download_wait_time)
        logger.info("Download completed.")

        # Unzip all contents in the same folder
        with zipfile.ZipFile(target_file, "r") as zip_ref:
            zip_ref.extractall(target_dir)

        # Remove the zip file
        os.remove(target_file)
        logger.info("Unzipping completed.")

    except Exception as e:
        logger.error(e)

    finally:
        # Close the browser after completion or in case of an error
        driver.quit()
        logger.info("Download completed.")

if __name__ == "__main__":
    main()
