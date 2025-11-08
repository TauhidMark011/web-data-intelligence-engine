from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

class MultiPageScraper:
    def __init__(self):
        self.setup_driver()
        self.all_data = []
        
    def setup_driver(self):
        """Setup Chrome driver"""
        print("Setting up Chrome driver...")
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        print("✅ Chrome driver setup successfully!")
    
    def scrape_pagination_site(self, base_url, max_pages=3):
        """
        Scrape a website with pagination (multiple pages)
        """
        print(f"Scraping pagination site: {base_url}")
        
        current_page = 1
        
        while current_page <= max_pages:
            print(f"\n Processing page {current_page}/{max_pages}...")
            
            # Navigate to current page
            if current_page == 1:
                url = base_url
            else:
                url = f"{base_url}?page={current_page}"  # Adjust based on site structure
            
            try:
                self.driver.get(url)
                
                # Wait for content to load
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Extract data from current page
                page_data = self.extract_page_data(current_page)
                self.all_data.extend(page_data)
                
                print(f"Page {current_page}: Found {len(page_data)} items")
                
                # Try to find and click next page button
                if not self.go_to_next_page():
                    print("No more pages found")
                    break
                    
                current_page += 1
                time.sleep(2)  # Be respectful
                
            except Exception as e:
                print(f"Error on page {current_page}: {e}")
                break
        
        return self.all_data
    
    def extract_page_data(self, page_number):
        """Extract data from current page"""
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        page_data = []
        
        # Extract quotes (for quotes.toscrape.com)
        quotes = soup.find_all('div', class_='quote')
        for quote in quotes:
            try:
                text_elem = quote.find('span', class_='text')
                author_elem = quote.find('small', class_='author')
                tags_elems = quote.find_all('a', class_='tag')
                
                if text_elem and author_elem:
                    data = {
                        'page': page_number,
                        'type': 'quote',
                        'text': text_elem.get_text().strip(),
                        'author': author_elem.get_text().strip(),
                        'tags': ', '.join([tag.get_text() for tag in tags_elems]),
                        'source_url': self.driver.current_url
                    }
                    page_data.append(data)
            except Exception as e:
                print(f"Error extracting quote: {e}")
        
        # If no quotes found, extract general content
        if not page_data:
            paragraphs = soup.find_all('p')
            for i, p in enumerate(paragraphs[:10]):
                text = p.get_text().strip()
                if text and len(text) > 10:
                    page_data.append({
                        'page': page_number,
                        'type': 'paragraph',
                        'content': text,
                        'source_url': self.driver.current_url
                    })
        
        return page_data
    
    def go_to_next_page(self):
        """Try to navigate to next page"""
        try:
            # Look for next page button
            next_buttons = self.driver.find_elements(By.CSS_SELECTOR, 
                "li.next a, a.next, [rel='next'], .pagination .next a")
            
            for button in next_buttons:
                if button.is_displayed() and button.is_enabled():
                    button.click()
                    # Wait for next page to load
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    return True
                    
        except Exception as e:
            print(f"Could not find next page: {e}")
        
        return False
    
    def scrape_ecommerce_site(self, url):
        """
        Scrape e-commerce site with product listings
        """
        print(f"Scraping e-commerce site: {url}")
        
        self.driver.get(url)
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        products_data = []
        
        # Extract product information
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Look for product elements (adjust selectors based on site)
        products = soup.find_all(['div', 'article'], class_=lambda x: x and any(word in str(x).lower() for word in ['product', 'item', 'card']))
        
        for product in products[:10]:  # Limit to first 10 products
            try:
                name_elem = product.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', '.title', '.name'])
                price_elem = product.find(['.price', '.cost', '[class*="price"]'])
                desc_elem = product.find(['p', '.description', '.desc'])
                
                product_data = {
                    'type': 'product',
                    'name': name_elem.get_text().strip() if name_elem else 'N/A',
                    'price': price_elem.get_text().strip() if price_elem else 'N/A',
                    'description': desc_elem.get_text().strip() if desc_elem else 'N/A',
                    'source_url': self.driver.current_url
                }
                products_data.append(product_data)
                
            except Exception as e:
                print(f"Error extracting product: {e}")
        
        return products_data
    
    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()
            print("Browser closed")

def main():
    """Main function for multi-page scraping"""
    scraper = MultiPageScraper()
    
    try:
        # Test with different types of multi-page sites
        test_sites = [
            {
                'name': 'Quotes Pagination',
                'url': 'https://quotes.toscrape.com/',
                'type': 'pagination',
                'max_pages': 3
            },
            {
                'name': 'E-commerce Products', 
                'url': 'https://webscraper.io/test-sites/e-commerce/allinone',
                'type': 'ecommerce',
                'max_pages': 1
            }
        ]
        
        all_scraped_data = []
        
        for site in test_sites:
            print(f"\n{'='*60}")
            print(f" Testing: {site['name']}")
            print(f"🔗 URL: {site['url']}")
            print('='*60)
            
            if site['type'] == 'pagination':
                data = scraper.scrape_pagination_site(site['url'], site['max_pages'])
            else:
                data = scraper.scrape_ecommerce_site(site['url'])
            
            all_scraped_data.extend(data)
            print(f" {site['name']}: Collected {len(data)} total items")
            
            time.sleep(2)  # Be respectful
        
        # Save all data
        if all_scraped_data:
            df = pd.DataFrame(all_scraped_data)
            df.to_csv('multi_page_scraped_data.csv', index=False)
            print(f"\n✅ Successfully saved {len(df)} items to 'multi_page_scraped_data.csv'")
            
            # Show summary
            print("\n SCRAPING SUMMARY:")
            print(f"   Total items: {len(df)}")
            print(f"   Data types: {df['type'].value_counts().to_dict()}")
            if 'page' in df.columns:
                print(f"   Pages scraped: {df['page'].max()}")
            
            print("\n Sample data:")
            print(df.head())
        else:
            print("No data was scraped")
            
    except Exception as e:
        print(f"Main function error: {e}")
    
    finally:
        scraper.close()

if __name__ == "__main__":
    main()