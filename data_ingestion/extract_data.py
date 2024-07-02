import requests
import os
from google.cloud import storage
from bs4 import BeautifulSoup
from dotenv import load_dotenv

url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
load_dotenv()
bucket_name = os.getenv('GCS_BUCKET_NAME')

response = requests.get(url)
html_content = response.text
soup = BeautifulSoup(html_content, 'html.parser')
links = soup.find_all('a', title='Yellow Taxi Trip Records', href=True)

for link in links:
    filename = str(link['href']).split('/')[-1].strip()
    download_url = link['href'].strip()
    print(f'{filename} download started')
    with requests.get(download_url, stream = True) as response:
        response.raise_for_status()
        with open(filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size = 8192):
                file.write(chunk)
    print(f'File {filename} downloaded')
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'raw_pq/{filename}')
    blob.upload_from_filename(filename)
    print(f"Uploaded {filename} to GCS bucket {bucket_name}")

    os.remove(filename)
    print(f'File {filename} removed')