# Import necessary libraries
import pandas as pd  # For future data manipulation (currently unused)
import json  # For parsing embedded JSON data
import asyncio  # To support asynchronous execution
import nest_asyncio  # To allow nested event loops (important in Jupyter)
import re  # For regular expression operations
from playwright.async_api import async_playwright  # Playwright for web scraping
from datetime import datetime, timedelta  # For time manipulations
from dateutil.relativedelta import relativedelta  # To handle relative time differences like months

# Allow Playwright to run inside Jupyter or nested loops
nest_asyncio.apply()

class DetailsScraping:
    def __init__(self, url, retries=3):
        self.url = url  # URL of the page to scrape
        self.retries = retries  # How many times to retry on failure

    # Main function to extract all cards and their detailed info
    async def get_card_details(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Launch headless browser
            page = await browser.new_page()  # Create a new page instance

            # Set default timeouts for navigation and actions
            page.set_default_navigation_timeout(30000)
            page.set_default_timeout(30000)

            cards = []  # List to hold all collected car listings

            # Retry scraping to improve robustness
            for attempt in range(self.retries):
                try:
                    # Load the target URL
                    await page.goto(self.url, wait_until="domcontentloaded")
                    await page.wait_for_selector('.StackedCard_card__Kvggc', timeout=30000)

                    # Extract all card elements
                    card_cards = await page.query_selector_all('.StackedCard_card__Kvggc')
                    for card in card_cards:
                        # Extract summary details
                        link = await self.scrape_link(card)
                        card_type = await self.scrape_card_type(card)
                        title = await self.scrape_title(card)
                        pinned_today = await self.scrape_pinned_today(card)

                        # Extract in-depth details from the car's own page
                        scrape_more_details = await self.scrape_more_details(link)

                        # Consolidate all data into one dictionary
                        cards.append({
                            'id': scrape_more_details.get('id'),
                            'date_published': scrape_more_details.get('date_published'),
                            'relative_date': scrape_more_details.get('relative_date'),
                            'pin': pinned_today,
                            'type': card_type,
                            'title': title,
                            'description': scrape_more_details.get('description'),
                            'link': link,
                            'image': scrape_more_details.get('image'),
                            'price': scrape_more_details.get('price'),
                            'address': scrape_more_details.get('address'),
                            'additional_details': scrape_more_details.get('additional_details'),
                            'specifications': scrape_more_details.get('specifications'),
                            'views_no': scrape_more_details.get('views_no'),
                            'submitter': scrape_more_details.get('submitter'),
                            'ads': scrape_more_details.get('ads'),
                            'membership': scrape_more_details.get('membership'),
                            'phone': scrape_more_details.get('phone'),
                        })
                    break  # Exit retry loop on success

                except Exception as e:
                    # Print error and retry if needed
                    print(f"Attempt {attempt + 1} failed for {self.url}: {e}")
                    if attempt + 1 == self.retries:
                        print(f"Max retries reached for {self.url}. Returning partial results.")
                        break
                finally:
                    await page.close()  # Ensure page cleanup
                    if attempt + 1 < self.retries:
                        page = await browser.new_page()  # Start a fresh page on next attempt

            await browser.close()
            return cards  # Return all collected cards

    # Scrape the href link for individual car details
    async def scrape_link(self, card):
        rawlink = await card.get_attribute('href')
        base_url = 'https://www.q84sale.com'
        return f"{base_url}{rawlink}" if rawlink else None

    # Scrape the category/type of the car
    async def scrape_card_type(self, card):
        selector = '.text-6-med.text-neutral_600.styles_category__NQAci'
        element = await card.query_selector(selector)
        return await element.inner_text() if element else None

    # Scrape the title/name of the car listing
    async def scrape_title(self, card):
        selector = '.text-4-med.text-neutral_900.styles_title__l5TTA.undefined'
        element = await card.query_selector(selector)
        return await element.inner_text() if element else None

    # Scrape the description from the detail page
    async def scrape_description(self, page):
        selector = '.styles_description__DpRnU'
        element = await page.query_selector(selector)
        return await element.inner_text() if element else "No Description"

    # Check if the post is pinned today
    async def scrape_pinned_today(self, card):
        selector = '.StackedCard_tags__SsKrH'
        element = await card.query_selector(selector)

        if element:
            content = await element.inner_html()
            if content.strip() != "":
                return "Pinned today"

        return "Not Pinned"

    # Scrape the relative time (e.g., "قبل 5 دقائق")
    async def scrape_relative_date(self, page):
        try:
            parent_locator = page.locator('.d-flex.styles_topData__Sx1GF')
            await parent_locator.wait_for(state="visible", timeout=10000)

            data_items = page.locator('.d-flex.align-items-center.styles_dataWithIcon__For9u')
            items = await data_items.all()

            for item in items:
                text = await item.inner_text()
                if any(word in text for word in ['منذ', 'ساعة', 'يوم', 'دقيقة', 'شهر']):
                    time_element = await item.locator('.text-5-regular.m-text-6-med.text-neutral_600').inner_text()
                    return time_element.strip()
            return None

        except Exception as e:
            print(f"Error while scraping relative_date: {e}")
            return None

    # Convert the relative date string into an actual timestamp
    async def scrape_publish_date(self, relative_time):
        relative_time_pattern = r'(\d+)\s+(Second|Minute|Hour|Day|Month|شهر|ثانية|دقيقة|ساعة|يوم)'
        match = re.search(relative_time_pattern, relative_time, re.IGNORECASE)
        if not match:
            return "Invalid Relative Time"

        number = int(match.group(1))
        unit = match.group(2).lower()
        current_time = datetime.now()

        if unit in ["second", "ثانية"]:
            publish_time = current_time - timedelta(seconds=number)
        elif unit in ["minute", "دقيقة"]:
            publish_time = current_time - timedelta(minutes=number)
        elif unit in ["hour", "ساعة"]:
            publish_time = current_time - timedelta(hours=number)
        elif unit in ["day", "يوم"]:
            publish_time = current_time - timedelta(days=number)
        elif unit in ["month", "شهر"]:
            publish_time = current_time - relativedelta(months=number)
        else:
            return "Unsupported time unit found."

        return publish_time.strftime("%Y-%m-%d %H:%M:%S")

    # Extract the number of views from the details page
    async def scrape_views_no(self, page):
        try:
            views_selector = '.d-flex.align-items-center.styles_dataWithIcon__For9u .text-5-regular.m-text-6-med.text-neutral_600'
            views_element = await page.query_selector(views_selector)

            if views_element:
                views_no = await views_element.inner_text()
                return views_no.strip()
            else:
                print(f"Views element not found using selector: {views_selector}")
                return None
        except Exception as e:
            print(f"Error while scraping views number: {e}")
            return None

    # Extract the ad ID from the page
    async def scrape_id(self, page):
        parent_selector = '.el-lvl-1.d-flex.align-items-center.justify-content-between.styles_sectionWrapper__v97PG'
        parent_element = await page.query_selector(parent_selector)
        if not parent_element:
            print("Parent element not found")
            return None

        ad_id_selector = '.text-4-regular.m-text-5-med.text-neutral_600'
        ad_id_element = await parent_element.query_selector(ad_id_selector)
        if not ad_id_element:
            print("Ad ID element not found within parent")
            return None

        text = await ad_id_element.inner_text()
        match = re.search(r'رقم الاعلان:\s*(\d+)', text)
        if match:
            return match.group(1)
        else:
            print("Regex did not match")
        return None

    # Scrape the image URL of the car
    async def scrape_image(self, page):
        try:
            image_selector = '.styles_img__PC9G3'
            image = await page.query_selector(image_selector)
            return await image.get_attribute('src') if image else None
        except Exception as e:
            print(f"Error scraping image: {e}")
            return None

    # Scrape the price text
    async def scrape_price(self, page):
        price_selector = '.h3.m-h5.text-prim_4sale_500'
        price = await page.query_selector(price_selector)
        return await price.inner_text() if price else "0 KWD"

    # Extract the address of the car listing
    async def scrape_address(self, page):
        address_selector = '.text-4-regular.m-text-5-med.text-neutral_600'
        address = await page.query_selector(address_selector)
        if address:
            text = await address.inner_text()
            if re.match(r'^رقم الاعلان: \d+$', text):
                return "Not Mentioned"
            return text
        return "Not Mentioned"

    # Scrape features such as "sunroof", "leather seats", etc.
    async def scrape_additionalDetails_list(self, page):
        selector = '.styles_boolAttrs__Ce6YV .styles_boolAttr__Fkh_j div'
        elements = await page.query_selector_all(selector)

        values_list = []
        for element in elements:
            text = await element.inner_text()
            if text.strip():
                values_list.append(text.strip())

        return values_list

    # Scrape structured specifications such as year, model, etc.
    async def scrape_specifications(self, page):
        selector = '.styles_attrs__PX5Fs .styles_attr__BN3w_'
        elements = await page.query_selector_all(selector)

        attributes = {}
        for element in elements:
            img_element = await element.query_selector('img')
            if img_element:
                alt_text = await img_element.get_attribute('alt')
                text_element = await element.query_selector('.text-4-med.m-text-5-med.text-neutral_900')
                if text_element:
                    value = await text_element.inner_text()
                    if alt_text and value:
                        attributes[alt_text] = value.strip()

        return attributes

    # Extract the phone number from embedded JSON data
    async def scrape_phone_number(self, page):
        try:
            script_content = await page.inner_html('script#__NEXT_DATA__')

            if script_content:
                data = json.loads(script_content.strip())
                phone_number = data.get("props", {}).get("pageProps", {}).get("listing", {}).get("phone", None)
                return phone_number if phone_number else None
            else:
                print("Script tag with id '__NEXT_DATA__' not found.")
                return None

        except Exception as e:
            print(f"Error while scraping phone number: {e}")
            return None

    # Scrape submitter name, ad count, and membership info
    async def scrape_submitter_details(self, page):
        info_wrapper_selector = '.styles_infoWrapper__v4P8_.undefined.align-items-center'
        info_wrappers = await page.query_selector_all(info_wrapper_selector)

        if len(info_wrappers) > 0:
            second_div = info_wrappers[0]
            submitter_selector = '.text-4-med.m-h6.text-neutral_900'
            submitter_element = await second_div.query_selector(submitter_selector)
            submitter = await submitter_element.inner_text() if submitter_element else None

            details_selector = '.styles_memberDate__qdUsm span.text-neutral_600'
            detail_elements = await second_div.query_selector_all(details_selector)

            ads = "0 ads"
            membership = "membership year not mentioned"

            for detail_element in detail_elements:
                detail_text = await detail_element.inner_text()
                if re.match(r'^\d+\s+ads$', detail_text, re.IGNORECASE) or re.match(r'^\d+\s+اعلان$', detail_text, re.IGNORECASE):
                    ads = detail_text
                elif re.match(r'^عضو منذ \D+\s+\d+$', detail_text) or re.match(r'^member since \D+\s+\d+$', detail_text, re.IGNORECASE):
                    membership = detail_text

            return {
                'submitter': submitter,
                'ads': ads,
                'membership': membership
            }
        return {}

    # Aggregated method that scrapes all detailed data for a single listing
    async def scrape_more_details(self, url):
        retries = 3
        for attempt in range(retries):
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()

                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                    id = await self.scrape_id(page)
                    description = await self.scrape_description(page)
                    image = await self.scrape_image(page)
                    price = await self.scrape_price(page)
                    address = await self.scrape_address(page)
                    additional_details = await self.scrape_additionalDetails_list(page)
                    specifications = await self.scrape_specifications(page)
                    views_no = await self.scrape_views_no(page)
                    submitter_details = await self.scrape_submitter_details(page)
                    phone = await self.scrape_phone_number(page)
                    relative_date = await self.scrape_relative_date(page)
                    date_published = await self.scrape_publish_date(relative_date) if relative_date else None

                    details = {
                        'id': id,
                        'description': description,
                        'image': image,
                        'price': price,
                        'address': address,
                        'additional_details': additional_details,
                        'specifications': specifications,
                        'views_no': views_no,
                        'submitter': submitter_details.get('submitter'),
                        'ads': submitter_details.get('ads'),
                        'membership': submitter_details.get('membership'),
                        'phone': phone,
                        'relative_date': relative_date,
                        'date_published': date_published,
                    }

                    await browser.close()
                    return details

            except Exception as e:
                print(f"Error while scraping more details from {url}: {e}")
                if attempt + 1 == retries:
                    print(f"Max retries reached for {url}. Returning partial results.")
                    return {}

        return {}
