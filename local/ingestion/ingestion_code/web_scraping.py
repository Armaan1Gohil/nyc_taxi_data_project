from bs4 import BeautifulSoup
import requests
import logging

logger = logging.getLogger(__name__)

def get_latest_parquet_links(page_url, num_links):
    logger.info(f"Fetching the latest {num_links} links from {page_url}")
    html_response = requests.get(page_url)
    html_response.raise_for_status()

    soup = BeautifulSoup(html_response.text, 'html.parser')
    headers = soup.find_all('a', attrs={'title': 'Yellow Taxi Trip Records'})

    latest_links = [headers[i].get('href').strip() for i in range(num_links)]
    logger.info(f"Found {len(latest_links)} links.")
    return latest_links