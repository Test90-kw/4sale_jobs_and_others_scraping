# Required imports
import asyncio  # For async operations
import pandas as pd  # For working with tabular data
import os  # For environment variable and file path handling
import json  # For parsing JSON-formatted credentials
import logging  # For logging info and errors
from datetime import datetime, timedelta  # For handling dates
from typing import Dict, List, Tuple  # Type hinting
from pathlib import Path  # To manage file system paths
from DetailsScraper import DetailsScraping  # Scraper class to fetch data from a URL
from SavingOnDriveOthers import SavingOnDriveOthers  # Handles uploading to Google Drive


class OthersMainScraper:
    def __init__(self, others_data: Dict[str, List[Tuple[str, int]]]):
        self.others_data = others_data  # Dictionary mapping category names to URLs and page counts
        self.chunk_size = 2  # Number of categories to process at once (in parallel chunks)
        self.max_concurrent_links = 2  # Max concurrent page scrapes
        self.logger = logging.getLogger(__name__)  # Logger for this class
        self.setup_logging()  # Setup log output to file and console
        self.temp_dir = Path("temp_files")  # Temp folder to store Excel files
        self.temp_dir.mkdir(exist_ok=True)  # Create the temp folder if it doesn't exist
        self.upload_retries = 3  # Number of times to retry uploading to Drive
        self.upload_retry_delay = 15  # Seconds between retry attempts
        self.page_delay = 3  # Delay between scraping pages
        self.chunk_delay = 10  # Delay between chunks

    def setup_logging(self):
        """Initialize logging configuration."""
        stream_handler = logging.StreamHandler()  # Console output
        file_handler = logging.FileHandler("scraper.log")  # File log output

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[stream_handler, file_handler],
        )
        self.logger.setLevel(logging.INFO)
        print("Logging setup complete.")

    async def scrape_other(self, other_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> List[Dict]:
        """Scrape data for a single category."""
        self.logger.info(f"Starting to scrape {other_name}")
        card_data = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # Target date filter

        async with semaphore:
            for url_template, page_count in urls:
                for page in range(1, page_count + 1):
                    url = url_template.format(page)  # Build URL by inserting page number
                    scraper = DetailsScraping(url)  # Instantiate scraper
                    try:
                        cards = await scraper.get_card_details()  # Fetch data from page
                        for card in cards:
                            # Only include listings from yesterday
                            if card.get("date_published") and card.get("date_published", "").split()[0] == yesterday:
                                card_data.append(card)

                        await asyncio.sleep(self.page_delay)  # Delay between pages
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")
                        continue

        return card_data

    async def save_to_excel(self, other_name: str, card_data: List[Dict]) -> str:
        """Save scraped data to an Excel file."""
        if not card_data:
            self.logger.info(f"No data to save for {other_name}, skipping Excel file creation.")
            return None

        safe_name = other_name.replace('/', '_').replace('\\', '_')  # Replace invalid characters in filename
        excel_file = Path(f"{safe_name}.xlsx")

        try:
            df = pd.DataFrame(card_data)  # Convert data to DataFrame
            df.to_excel(excel_file, index=False)  # Save DataFrame to Excel
            self.logger.info(f"Successfully saved data for {other_name}")
            return str(excel_file)
        except Exception as e:
            self.logger.error(f"Error saving Excel file {excel_file}: {e}")
            return None

    async def upload_files_with_retry(self, drive_saver, files: List[str]) -> List[str]:
        """Upload files to Google Drive with retry mechanism."""
        uploaded_files = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            self.logger.info(f"Checking local files before upload: {files}")
            for file in files:
                self.logger.info(f"File {file} exists: {os.path.exists(file)}, size: {os.path.getsize(file) if os.path.exists(file) else 'N/A'}")

            # Get folder for yesterday, or create it if it doesn't exist
            folder_id = drive_saver.get_folder_id(yesterday)
            if not folder_id:
                self.logger.info(f"Creating new folder for date: {yesterday}")
                folder_id = drive_saver.create_folder(yesterday)
                if not folder_id:
                    raise Exception("Failed to create or get folder ID")
                self.logger.info(f"Created new folder '{yesterday}' with ID: {folder_id}")

            # Try uploading each file, with retry mechanism
            for file in files:
                for attempt in range(self.upload_retries):
                    try:
                        if os.path.exists(file):
                            file_id = drive_saver.upload_file(file, folder_id)
                            if not file_id:
                                raise Exception("Upload returned no file ID")
                            uploaded_files.append(file)
                            self.logger.info(f"Successfully uploaded {file} with ID: {file_id}")
                            break
                        else:
                            self.logger.error(f"File not found for upload: {file}")
                            break
                    except Exception as e:
                        self.logger.error(f"Upload attempt {attempt + 1} failed for {file}: {e}")
                        if attempt < self.upload_retries - 1:
                            self.logger.info(f"Retrying after {self.upload_retry_delay} seconds...")
                            await asyncio.sleep(self.upload_retry_delay)
                            drive_saver.authenticate()  # Re-authenticate before retry
                        else:
                            self.logger.error(f"Failed to upload {file} after {self.upload_retries} attempts")

        except Exception as e:
            self.logger.error(f"Error in upload process: {e}")
            raise

        return uploaded_files

    async def scrape_all_others(self):
        """Scrape all categories and handle uploads."""
        self.temp_dir.mkdir(exist_ok=True)  # Ensure temp folder exists

        # Setup Google Drive
        try:
            credentials_json = os.environ.get("OTHERS_GCLOUD_KEY_JSON")  # Get credentials from env var
            if not credentials_json:
                raise EnvironmentError("OTHERS_GCLOUD_KEY_JSON environment variable not found")
            else:
                self.logger.info("Environment variable OTHERS_GCLOUD_KEY_JSON is set.")

            credentials_dict = json.loads(credentials_json)  # Parse JSON credentials
            drive_saver = SavingOnDriveOthers(credentials_dict)
            drive_saver.authenticate()  # Authenticate with Google Drive

            self.logger.info("Testing Drive API access...")
            try:
                # Test parent folder access
                drive_saver.service.files().get(fileId=drive_saver.parent_folder_id).execute()
                self.logger.info("Successfully accessed parent folder")
            except Exception as e:
                self.logger.error(f"Failed to access parent folder: {e}")
                return
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        # Split work into chunks
        others_chunks = [
            list(self.others_data.items())[i : i + self.chunk_size]
            for i in range(0, len(self.others_data), self.chunk_size)
        ]

        semaphore = asyncio.Semaphore(self.max_concurrent_links)

        for chunk_index, chunk in enumerate(others_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(others_chunks)}")

            tasks = []
            for other_name, urls in chunk:
                task = asyncio.create_task(self.scrape_other(other_name, urls, semaphore))
                tasks.append((other_name, task))
                await asyncio.sleep(2)  # Delay between launching each task

            pending_uploads = []
            for other_name, task in tasks:
                try:
                    card_data = await task  # Await result of scraping
                    if card_data:
                        excel_file = await self.save_to_excel(other_name, card_data)
                        if excel_file:
                            pending_uploads.append(excel_file)
                except Exception as e:
                    self.logger.error(f"Error processing {other_name}: {e}")

            # Upload all collected Excel files
            if pending_uploads:
                await self.upload_files_with_retry(drive_saver, pending_uploads)

                # Cleanup local files
                for file in pending_uploads:
                    try:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {e}")

            # Wait before starting next chunk
            if chunk_index < len(others_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)


# Data categories to scrape, with their URL templates and number of pages
if __name__ == "__main__":
    others_data = {
        "عملات و طوابع و تحف قديمه": [("https://www.q84sale.com/ar/others/currencies-stamps-and-antiques/{}", 1)],
        "ادوات موسيقية": [("https://www.q84sale.com/ar/others/audio-and-musical/{}", 1)],
        "اللوازم المدرسية": [("https://www.q84sale.com/ar/others/school-supplies/{}", 1)],
        "كتب": [("https://www.q84sale.com/ar/others/books/{}", 1)],
        "مبيعات الجملة": [("https://www.q84sale.com/ar/others/wholesale/{}", 1)],
        "مطبوعات": [("https://www.q84sale.com/ar/others/stickers/{}", 1)],
        "مفقودات": [("https://www.q84sale.com/ar/others/lost-and-found/{}", 1)],
        "متفرقات أخرى": [("https://www.q84sale.com/ar/others/other-miscellaneous/{}", 3)],
    }

    # Main entry point: start scraping process
    async def main():
        scraper = OthersMainScraper(others_data)
        await scraper.scrape_all_others()

    asyncio.run(main())  # Run the async main function
