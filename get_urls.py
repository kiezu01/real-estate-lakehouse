import pandas as pd
import requests
from bs4 import BeautifulSoup
from csv import writer
import time
import random
import os
import logging

def get_user_agent():
    # Set a random user-agent to mimic different browsers
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_6_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15"
    ]
    return random.choice(user_agents)

def make_request(url):
    # Set a random user-agent for each request
    user_agent = get_user_agent()
    headers = {'User-Agent': user_agent}
    
    # Make the request using requests module instead of Selenium
    response = requests.get(url, headers=headers)
    return response

def extract_urls(response):
    # Parse the page content
    soup = BeautifulSoup(response.content, 'html.parser')
    
    urls = []
    # Extract URLs
    for link in soup.find_all('a', class_='AdItem_adItem__gDDQT'):
        href = link.get('href')
        if href:
            urls.append(href)
    
    return urls

def write_urls_to_csv(urls, csv_file):
    # Combine existing URLs with new URLs and remove duplicates
    existing_urls = set()
    if os.path.exists(csv_file):
        with open(csv_file, 'r', encoding='utf-8') as f:
            existing_urls = {line.strip() for line in f}

    all_urls = existing_urls.union(urls)

    # Write combined URLs back to CSV
    with open(csv_file, 'w', encoding='utf-8', newline='') as f:
        csv_writer = writer(f)
        for url in all_urls:
            csv_writer.writerow([url])
    
    
    
def process_pages(start_page, end_page):
    base_url = 'https://www.nhatot.com/mua-ban-bat-dong-san?page={}'
    
    for i in range(start_page, end_page):
        url = base_url.format(i)
        logging.info(f"Processing page {i}...")
        
        response = make_request(url)
        if response.status_code == 200:
            urls = extract_urls(response)
            write_urls_to_csv(urls, 'crawler/data/urls4.csv')
            
            time.sleep(random.uniform(1, 5))  # Random delay between requests
        else:
            logging.error(f"Failed to fetch page {i}. Status code: {response.status_code}")
            continue
    
    logging.info("All pages processed!")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Define start and end page
    start_page = 201
    end_page = 1000
    
    process_pages(start_page, end_page)
    logging.info("Duplicates dropped successfully and file saved")
    logging.info("All Done!")

if __name__ == "__main__":
    main()
