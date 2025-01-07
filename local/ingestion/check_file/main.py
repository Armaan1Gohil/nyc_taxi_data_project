# from date_tracker import DateTracker
from web_scraper import WebScraper

def main():
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

    web_scraper = WebScraper(page_url)

    tags = web_scraper.get_links(title_filter="Yellow Taxi Trip Records")

    for tag in tags:
        link = tag.get_link(tag)


if __name__ == "__main__":
    print(main())