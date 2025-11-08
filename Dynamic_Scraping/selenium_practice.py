from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

class DynamicContentScraper:
    def __init__(self):
        """Initialize the Selenium scraper"""
        self.driver = None
        self.setup_driver()
        
    def setup_driver(self):
        """Setup Chrome driver with options"""
        print("Setting up Chrome driver...")
        
        chrome_options = Options()
        
        # Uncomment the next line if you want to run in background (headless)
        # chrome_options.add_argument("--headless")  
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")

        try:
        # This often works without specifying path
              self.driver = webdriver.Chrome(options=chrome_options)
              print("Chrome driver setup successfully!")
        except Exception as e:
              print(f"Error: {e}")
              print("Make sure chromedriver.exe is in your PATH or project folder")
        # try:
        #     # For Windows - make sure chromedriver.exe is in your project folder
        #     service = Service("chromedriver.exe")
        #     self.driver = webdriver.Chrome(service=service, options=chrome_options)
        #     print("Chrome driver setup successfully!")
        # except Exception as e:
        #     print(f"Error setting up driver: {e}")
        #     print("Please download chromedriver from: https://chromedriver.chromium.org/")
        #     print(" Place chromedriver.exe in your project folder")
    
    def scrape_infinite_scroll_site(self, url):
        """
        Scrape a website with infinite scroll
        This is perfect for practice - content loads as you scroll
        """
        print(f"Scraping infinite scroll site: {url}")
        
        try:
            # Navigate to page
            self.driver.get(url)
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Let's see what's initially loaded
            initial_content = self.get_page_content()
            print(f"Initial content items: {len(initial_content)}")
            
            # Now let's scroll to load more content
            print("Starting to scroll...")
            additional_content = self.scroll_and_collect()
            
            # Combine all content
            all_content = initial_content + additional_content
            
            print(f"Total content collected: {len(all_content)} items")
            return all_content
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return []
    
    def get_page_content(self):
        """Get current page content"""
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Extract various elements that might contain data
        content = []
        
        # Get all paragraphs
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text().strip()
            if text and len(text) > 5:
                content.append({'type': 'paragraph', 'content': text})
        
        # Get all headings
        headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        for h in headings:
            text = h.get_text().strip()
            if text:
                content.append({'type': 'heading', 'content': text})
        
        # Get all list items
        list_items = soup.find_all('li')
        for li in list_items[:20]:  # Limit to avoid too much data
            text = li.get_text().strip()
            if text and len(text) > 3:
                content.append({'type': 'list_item', 'content': text})
        
        return content
    
    def scroll_and_collect(self, max_scrolls=3):
        """
        Scroll down to load more dynamic content
        """
        print(f"Scrolling {max_scrolls} times to load dynamic content...")
        
        all_additional_content = []
        
        for scroll in range(max_scrolls):
            print(f"   Scroll {scroll + 1}/{max_scrolls}")
            
            # Scroll to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait for new content to load
            time.sleep(2)
            
            # Get new content that loaded after scroll
            new_content = self.get_page_content()
            all_additional_content.extend(new_content)
            
            print(f"   Found {len(new_content)} new items")
        
        # Remove duplicates
        unique_content = []
        seen_content = set()
        
        for item in all_additional_content:
            content_hash = hash(item['content'])
            if content_hash not in seen_content:
                seen_content.add(content_hash)
                unique_content.append(item)
        
        return unique_content
    
    def scrape_ajax_site(self, url):
        """
        Scrape a site that loads content via AJAX after initial load
        """
        print(f"🔄 Scraping AJAX site: {url}")
        
        try:
            self.driver.get(url)
            
            # Wait longer for AJAX content to load
            wait = WebDriverWait(self.driver, 15)
            
            # Wait for specific elements that indicate content is loaded
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Sometimes we need to wait for specific loading indicators to disappear
            time.sleep(3)
            
            content = self.get_page_content()
            print(f"AJAX content collected: {len(content)} items")
            
            return content
            
        except Exception as e:
            print(f" Error scraping AJAX site: {e}")
            return []
    
    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()
            print("Browser closed")

def main():
    """Main function to run Selenium scraping"""
    scraper = DynamicContentScraper()
    
    try:
        # Practice URLs with different types of dynamic content
        practice_urls = [
            {
                'name': 'Infinite Scroll Practice',
                'url': 'https://quotes.toscrape.com/scroll',
                'type': 'infinite_scroll'
            },
            {
                'name': 'AJAX Content Practice', 
                'url': 'https://webscraper.io/test-sites/e-commerce/ajax',
                'type': 'ajax'
            }
        ]
        
        all_results = []
        
        for site in practice_urls:
            print(f"\n{'='*50}")
            print(f"Testing: {site['name']}")
            print(f"🔗 URL: {site['url']}")
            print(f" Type: {site['type']}")
            print('='*50)
            
            if site['type'] == 'infinite_scroll':
                content = scraper.scrape_infinite_scroll_site(site['url'])
            else:
                content = scraper.scrape_ajax_site(site['url'])
            
            # Add site info to each content item
            for item in content:
                item['source_url'] = site['url']
                item['site_name'] = site['name']
            
            all_results.extend(content)
            
            # Be respectful - wait between requests
            time.sleep(2)
        
        # Save results
        if all_results:
            df = pd.DataFrame(all_results)
            df.to_csv('dynamic_content_scraped.csv', index=False)
            print(f"\n✅ Successfully saved {len(df)} items to 'dynamic_content_scraped.csv'")
            
            # Display sample
            print("\n Sample of scraped data:")
            print(df.head(10))
        else:
            print("No data was scraped")
        
    except Exception as e:
        print(f"Main function error: {e}")
    
    finally:
        scraper.close()

if __name__ == "__main__":
    main()