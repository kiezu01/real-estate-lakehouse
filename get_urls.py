import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from csv import writer
import time
import random

# Define start and end page
start_page = 970
end_page = 1000

base_url = 'https://www.nhatot.com/mua-ban-bat-dong-san?page={}'

# Initialize WebDriver outside the loop
driver = webdriver.Chrome()

# Set a random user-agent to mimic different browsers
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_6_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15"
]

with open('crawler/data/urls.csv', 'a', encoding='utf-8', newline='') as f:
    csv_writer = writer(f)
    
    for i in range(start_page, end_page):
        url = base_url.format(i)
        print(f"Processing page {i}...")
        
        # Set a random user-agent for each request
        user_agent = random.choice(user_agents)
        headers = {'User-Agent': user_agent}
        
        # Make the request using requests module instead of Selenium
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # Parse the page content
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract URLs and write to CSV
            for link in soup.find_all('a', class_='AdItem_adItem__gDDQT'):
                href = link.get('href')
                if href:
                    csv_writer.writerow([href])
                    
            time.sleep(random.uniform(3, 10))  # Random delay between requests
        else:
            print(f"Failed to fetch page {i}. Status code: {response.status_code}")
            continue

# Quit WebDriver after all pages are processed
driver.quit()

print("All Done!")