import concurrent.futures
import time
import pandas as pd
import requests
import os
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random
from selenium import webdriver

# logging configuration
logging.basicConfig(filename='crawler.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# define the necessary variables
directory = 'data/'
ua = UserAgent()  # initialize UserAgent once
base_url = 'https://www.nhatot.com'

def input_file():
    return os.path.join(directory, 'urls.csv')

def extract_urls(start_row, end_row):
    list_of_urls = pd.read_csv(input_file(), header=None, names=['url'])
    urls = list_of_urls['url'][start_row:end_row].tolist()
    # Concatenate base URL if necessary
    urls = [url if url.startswith('http://') or url.startswith('https://') else f'{base_url}/{url}' for url in urls]
    return urls

def output_file(data_type):
    if data_type == 'data':
        return os.path.join(directory, 'house_info_2.csv')
    elif data_type == 'error':
        return os.path.join(directory, 'error_links.csv')
    
def extract_id(url):
    return url.split('/')[-1].split('.')[0]

def polite_request(url):
    headers = {'User-Agent': ua.random}
    try:
        time.sleep(random.randint(1, 3))
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            sleep_time = int(response.headers.get("Retry-After", 15))
            logging.info(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)
            return polite_request(url)
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

def scrape_url(link):
    response = polite_request(link)
    if response and response.status_code == 200:
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(options=options)
    
    # Mở URL
        driver.get(link)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        house_info = soup.find('div', class_='DetailView_adviewCointainer__rdzwn')  # Adjust class as needed
        if house_info:
            house_id = extract_id(link)
            house = {
                # Extract information from the page
                    'id': house_id,
                    'link': link,
                    'house_name': house_info.find('h1', class_='AdDecriptionVeh_adTitle__vEuKD').text.strip() if house_info.find('h1', class_='AdDecriptionVeh_adTitle__vEuKD') else "",
                    'address': house_info.find('span', class_='fz13').text.strip() if house_info.find('span', class_='fz13') else "",
                    'price': house_info.find('span', itemprop='price').text.split('-')[0].strip() if house_info.find('span', itemprop='price') else "",
                    'area': house_info.find('span', itemprop='size').text.strip() if house_info.find('span', itemprop='size') else "",
                    'length': house_info.find('span', itemprop='length').text.strip() if house_info.find('span', itemprop='length') else "",
                    'width': house_info.find('span', itemprop='width').text.strip() if house_info.find('span', itemprop='width') else "",
                    'price_per_square': house_info.find('span', itemprop='price_m2').text.strip() if house_info.find('span', itemprop='price_m2') else "",
                    'bedroom': house_info.find('span', itemprop='rooms').text.strip() if house_info.find('span', itemprop='rooms') else "",
                    'floor': house_info.find('span', itemprop='floors').text.strip() if house_info.find('span', itemprop='floors') else "",
                    'toilet': house_info.find('span', itemprop='toilets').text.strip() if house_info.find('span', itemprop='toilets') else "",
                    'house_type': house_info.find('span', itemprop='house_type').text.strip() if house_info.find('span', itemprop='house_type') else "",
                    'furnishing_status': house_info.find('span', itemprop='furnishing_sell').text.strip() if house_info.find('span', itemprop='furnishing_sell') else "",
                    'legal_status': house_info.find('span', itemprop='property_legal_document').text.strip() if house_info.find('span', itemprop='property_legal_document') else "",
                    'priority_characteristics': house_info.find('span', itemprop='pty_characteristics').text.strip() if house_info.find('span', itemprop='pty_characteristics') else "",
                    'living_size': house_info.find('span', itemprop='living_size').text.strip() if house_info.find('span', itemprop='living_size') else "",
                    'direction': house_info.find('span', itemprop='direction').text.strip() if house_info.find('span', itemprop='direction') else "",
                    'balconydirection': house_info.find('span', itemprop='balconydirection').text.strip() if house_info.find('span', itemprop='balconydirection') else "",
                    'block': house_info.find('span', itemprop='block').text.strip() if house_info.find('span', itemprop='block') else "",
                    'apartment_type': house_info.find('span', itemprop='apartment_type').text.strip() if house_info.find('span', itemprop='apartment_type') else "",
                    'floornumber': house_info.find('span', itemprop='floornumber').text.strip() if house_info.find('span', itemprop='floornumber') else "",
                    'apartment_feature': house_info.find('span', itemprop='apartment_feature').text.strip() if house_info.find('span', itemprop='apartment_feature') else "",
                    'property_status': house_info.find('span', itemprop='property_status').text.strip() if house_info.find('span', itemprop='property_status') else "",
                    'land_type': house_info.find('span', itemprop='land_type').text.strip() if house_info.find('span', itemprop='land_type') else "",
                    'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'time_info': house_info.find('span', class_='AdImage_imageCaptionText__ScM56').text.strip() if house_info.find('span', class_='AdImage_imageCaptionText__ScM56') else ""
                
            }
            logging.info("Scraping successful: %s", link)
            return house, None
        else:
            logging.warning("No house information found for %s", link)
            return None, 404
    elif response:
        logging.error("Failed to fetch page %s, status code: %s", link, response.status_code)
        return None, response.status_code
    else:
        return None, "Failed to make a request"

def process_url(link):
    result, error_code = scrape_url(link)
    return result if result else {'link': link, 'error_code': error_code}

def scrape_multiple_urls(urls, max_workers=1):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_url, url) for url in urls]
        results = []
        errors = []
        for future in concurrent.futures.as_completed(futures):
            data = future.result()
            if 'error_code' in data:
                errors.append(data)
            else:
                results.append(data)
        return results, errors

def save_to_csv(data, file_path, mode='a', header=False, encoding='utf-8-sig'):
    df = pd.DataFrame(data)
    df.to_csv(file_path, mode=mode, index=False, header=header, encoding= encoding)

def main():
    start_time = time.time()
    urls = extract_urls(1101, 1105)  # update the range of URLs to extract

    data_list, error_links = scrape_multiple_urls(urls)
    save_to_csv(data_list, output_file('data'), header=not os.path.exists(output_file('data')))
    save_to_csv(error_links, output_file('error'), header=not os.path.exists(output_file('error')))

    print('Done!')
    print("---processing times completed in %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    main()