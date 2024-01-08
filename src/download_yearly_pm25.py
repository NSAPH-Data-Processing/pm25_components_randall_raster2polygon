import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import hydra

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    """
    Download yearly V5 satellite PM2.5 data from Washington University's Atmospheric Composition Analysis Group.
    https://sites.wustl.edu/acag/datasets/surface-pm2-5/
    """

    # == target file and url for download
    url = cfg.yearly_pm25_url
    target_file = f"{cfg.file_suffix}{cfg.year}01-{cfg.year}12.nc"

    # == setup chrome driver
    # get the current working directory
    cwd = os.getcwd()

    # Set up Chrome options for headless mode and automatic downloads
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": f"{cwd}/data/input/satellite_pm25",
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )

    # Setting up the Selenium WebDriver for Chrome using webdriver_manager
    ChromeDriverManager().install()
    driver = webdriver.Chrome(options=chrome_options)

    # == download file
    try:
        # Navigate to the website
        driver.get(url)

        # Format the XPath string with the target file name
        xpath_expression = f"//a[@data-resin-target='openfile'][contains(text(), '{target_file}')]"

        # Wait for the link to be available and click it
        link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath_expression))
        )
        link.click()

        # Wait for the new page to load and find the download button
        download_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Download')]"))
        )
        download_button.click()

        # Wait 10 seconds to make sure the file has downloaded
        time.sleep(cfg.download_wait_time)

    finally:
        # Close the browser after completion or in case of an error
        driver.quit()

if __name__ == "__main__":
    main()