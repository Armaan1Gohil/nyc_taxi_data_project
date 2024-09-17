# web_scraper.py
import requests
from bs4 import BeautifulSoup

def scrape_urls(page_url):
    html_response = requests.get(page_url)
    html_response.raise_for_status()

    soup = BeautifulSoup(html_response.text, 'html.parser')
    return soup.find_all('a', attrs={'title': 'Yellow Taxi Trip Records'})
