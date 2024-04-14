import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

start_time = time.time()

# create user_agents
# user_agents = [
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
#     "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_6_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15"
# ]
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_6_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4692.71 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4692.71 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4692.71 Safari/537.36"
]
# Function to scrape data from a single URL
def scrape_url(link):
    try:
        user_agent = random.choice(user_agents)
        headers = {'User-Agent': user_agent}
        response = requests.get(link, headers=headers)
        response.raise_for_status()  # raise an exception for HTTP errors (4xx or 5xx)
        soup = BeautifulSoup(response.content, 'html.parser')
        house_info = soup.find('div', class_='DetailView_adviewCointainer__rdzwn')

        # fetch data
        if house_info:
            house_name = house_info.find('h1', class_='AdDecriptionVeh_adTitle__vEuKD').text.strip() if house_info.find('h1', class_='AdDecriptionVeh_adTitle__vEuKD') else ""
            address = house_info.find('span', class_='fz13').text.replace('Xem bản đồ', '').strip() if house_info.find('span', class_='fz13') else ""
            price = house_info.find('span', itemprop='price').text.split('-')[0].strip() if house_info.find('span', itemprop='price') else ""
            area = house_info.find('span', itemprop='size').text.strip() if house_info.find('span', itemprop='size') else ""
            length = house_info.find('span', itemprop='length').text.strip() if house_info.find('span', itemprop='length') else ""
            width = house_info.find('span', itemprop='width').text.strip() if house_info.find('span', itemprop='width') else ""
            price_per_square = house_info.find('span', itemprop='price_m2').text.strip() if house_info.find('span', itemprop='price_m2') else ""
            bedroom = house_info.find('span', itemprop='rooms').text.strip() if house_info.find('span', itemprop='rooms') else ""
            floor = house_info.find('span', itemprop='floors').text.strip() if house_info.find('span', itemprop='floors') else ""
            toilet = house_info.find('span', itemprop='toilets').text.strip() if house_info.find('span', itemprop='toilets') else ""
            house_type = house_info.find('span', itemprop='house_type').text.strip() if house_info.find('span', itemprop='house_type') else ""
            furnishing_status = house_info.find('span', itemprop='furnishing_sell').text.strip() if house_info.find('span', itemprop='furnishing_sell') else ""
            legal_status = house_info.find('span', itemprop='property_legal_document').text.strip() if house_info.find('span', itemprop='property_legal_document') else ""
            priority_characteristics = house_info.find('span', itemprop='pty_characteristics').text.strip() if house_info.find('span', itemprop='pty_characteristics') else ""
            living_size = house_info.find('span', itemprop='living_size').text.strip() if house_info.find('span', itemprop='living_size') else ""
            direction = house_info.find('span', itemprop='direction').text.strip() if house_info.find('span', itemprop='direction') else ""

            print("Scraping:", link)
            
            # return data and error code
            return [house_name, address, price, area, length, width, price_per_square, bedroom, floor, toilet, house_type, furnishing_status, legal_status, priority_characteristics, living_size, direction], None
        else:
            print(f"No house information found for {link}")
            return None, 404
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error {e.response.status_code} occurred while scraping {link}: {e.response.reason}")
        return None, e.response.status_code
    finally:
        time.sleep(random.randint(3, 6))
        
# Base URL
url = 'https://www.nhatot.com'

start_row = 0
end_row = 1000
# read file
list_of_urls = pd.read_csv('crawler/data/urls_test.csv', header=None, names=['url'])

selected_urls = list_of_urls[start_row:end_row]
# Create a list of URLs by concatenating the base URL with the path
# new_links = [url + path for path in selected_urls]
new_links = [url + path for path in selected_urls['url']]

# Scrape each URL one by one using requests
data_list = []
error_links = []
for link in new_links:
    result, error_code = scrape_url(link)
    if result is not None:
        data_list.append(result)
    else:
        error_links.append((link, error_code))

# Save data to CSV
df_house_info = pd.DataFrame(data_list, columns=['House Name', 'Address', 'Price', 'Area', 'Length', 'Width', 'Price Per Square', 'Bedroom', 'Floor', 'Toilet', 'House Type', 'Furnishing Status', 'Legal Status', 'Priority Characteristics', 'Living Size', 'Direction'])
df_house_info.to_csv('crawler/data/house_info.csv', index=False, encoding='utf-8-sig')

# Save error links to CSV
df_errors = pd.DataFrame(error_links, columns=['Error Link', 'Error Code'])
df_errors.to_csv('crawler/data/error_links.csv', index=False, encoding='utf-8-sig')

print('Done!')
# print time processing
print("---processing times completed in %s seconds ---" % (time.time() - start_time))
