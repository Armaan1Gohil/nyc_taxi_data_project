import requests
from bs4 import BeautifulSoup

class WebScraper:
    def __init__(self, url):
        self.url = url
    
    def get_links(self, title_filter=None):
        try:
            html_response = requests.get(self.url)
            html_response.raise_for_status()
        except Exception as e:
            raise

        soup = BeautifulSoup(html_response.text, 'html.parser')
        if title_filter:
            tags = soup.find_all('a', attrs={'title': title_filter})
            return [tag['href'].strip() for tag in tags]
        return soup.find_all('a')