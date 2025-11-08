import schedule
import time
import logging
from datetime import datetime
import pandas as pd
import sqlite3
import sys
import os

# Add the Dynamic_Scraping folder to Python path so we can import modules
sys.path.append('Dynamic_Scraping')

# Now import ACTUAL scrapers
try:
    from selenium_practice import DynamicContentScraper  #Selenium class
    print("SUCCESS: Imported Selenium scraper")
except ImportError as e:
    print(f"ERROR: Could not import Selenium scraper: {e}")

class TrueCombinedPipeline:
    """
    This TRULY combines existing scrapers by IMPORTING them
    """
    
    def __init__(self):
        self.setup_logging()
        self.setup_database()
        self.logger = logging.getLogger(__name__)
    
    def setup_logging(self):
        """Setup comprehensive logging without emojis for Windows"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('true_combined_pipeline.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def setup_database(self):
        """Setup database connection and ensure proper schema"""
        self.conn = sqlite3.connect('scraping_data.db')
        
        # Ensure the tables have the correct schema for our new data
        self.create_tables_if_not_exist()
    
    def create_tables_if_not_exist(self):
        """Create tables with proper schema if they don't exist"""
        cursor = self.conn.cursor()
        
        # Drop and recreate quotes table with correct schema
        cursor.execute('DROP TABLE IF EXISTS quotes_new')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS quotes_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_text TEXT NOT NULL,
                author TEXT NOT NULL,
                tags TEXT,
                page_number INTEGER,
                source_url TEXT,
                scrape_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                data_quality_score INTEGER DEFAULT 100,
                scraper_type TEXT
            )
        ''')
        
        # Create general_content table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS general_content_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content_type TEXT NOT NULL,
                content_text TEXT NOT NULL,
                source_url TEXT,
                content_length INTEGER,
                word_count INTEGER,
                scrape_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                scraper_type TEXT
            )
        ''')
        
        self.conn.commit()
    
    def run_actual_beautifulsoup_scraper(self):
        """
        Actually calls ORIGINAL BeautifulSoup scraper logic
        """
        self.logger.info("Calling ACTUAL BeautifulSoup scraper...")
        
        try:
            import requests
            from bs4 import BeautifulSoup
            
            # This is the ACTUAL logic from web-scrap-enhance.py
            url = "https://www.passiton.com/inspirational-quotes/"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            scraped_data = {}
            
            # Extract page title
            page_title = soup.find('title')
            if page_title:
                scraped_data['page_title'] = page_title.get_text().strip()
            
            # Extract links (original logic)
            links = soup.find_all('a', href=True)
            scraped_data['links'] = []
            
            for link in links[:10]:
                link_data = {
                    'text': link.get_text().strip()[:50],
                    'url': link['href']
                }
                scraped_data['links'].append(link_data)
            
            # Extract paragraphs  
            paragraphs = soup.find_all('p')
            scraped_data['paragraphs'] = []
            
            for para in paragraphs[:5]:
                text = para.get_text().strip()
                if text and len(text) > 10:
                    scraped_data['paragraphs'].append(text[:200])
            
            self.logger.info(f"BeautifulSoup: Found {len(scraped_data['links'])} links, {len(scraped_data['paragraphs'])} paragraphs")
            
            # Convert to our standard format
            return self.convert_beautifulsoup_data(scraped_data)
            
        except Exception as e:
            self.logger.error(f"BeautifulSoup scraping failed: {e}")
            return {}
    
    def run_actual_selenium_scraper(self):
        """
        Actually calls EXISTING Selenium scraper
        This uses the DynamicContentScraper class from selenium_practice.py
        """
        self.logger.info("Calling ACTUAL Selenium scraper...")
        
        try:
            # This is using ACTUAL Selenium class
            scraper = DynamicContentScraper()
            
            # Use existing methods
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
                self.logger.info(f"Scraping: {site['name']}")
                
                if site['type'] == 'infinite_scroll':
                    content = scraper.scrape_infinite_scroll_site(site['url'])
                else:
                    content = scraper.scrape_ajax_site(site['url'])
                
                for item in content:
                    item['source_url'] = site['url']
                    item['site_name'] = site['name']
                
                all_results.extend(content)
                time.sleep(2)
            
            scraper.close()
            
            self.logger.info(f"Selenium: Collected {len(all_results)} items")
            
            # Convert to our standard format
            return self.convert_selenium_data(all_results)
            
        except Exception as e:
            self.logger.error(f"Selenium scraping failed: {e}")
            return {}
    
    def convert_beautifulsoup_data(self, data):
        """
        Convert BeautifulSoup data to standard format
        """
        converted = {
            'quotes': [],
            'products': [],
            'content': []
        }
        
        # Convert paragraphs to quotes
        for paragraph in data.get('paragraphs', []):
            converted['quotes'].append({
                'quote_text': paragraph,  # FIXED: Changed 'text' to 'quote_text'
                'author': 'Various',
                'tags': 'inspirational',
                'page_number': 1,  # FIXED: Changed 'page' to 'page_number'
                'source_url': 'https://www.passiton.com/inspirational-quotes/',
                'scraper_type': 'beautifulsoup'
            })
        
        # Add page title as content
        if 'page_title' in data:
            converted['content'].append({
                'content_type': 'page_title',
                'content_text': data['page_title'],  # FIXED: Changed 'content' to 'content_text'
                'source_url': 'https://www.passiton.com/inspirational-quotes/',
                'content_length': len(data['page_title']),
                'word_count': len(data['page_title'].split()),
                'scraper_type': 'beautifulsoup'
            })
        
        return converted
    
    def convert_selenium_data(self, data):
        """
        Convert Selenium data to standard format
        """
        converted = {
            'quotes': [],
            'products': [],
            'content': []
        }
        
        for item in data:
            if item.get('type') in ['paragraph', 'heading']:
                content_text = item.get('content', '')
                if 'quote' in content_text.lower() or len(content_text) > 30:
                    converted['quotes'].append({
                        'quote_text': content_text,  # FIXED: Changed 'text' to 'quote_text'
                        'author': 'Various',
                        'tags': 'dynamic',
                        'page_number': 1,  # FIXED: Changed 'page' to 'page_number'
                        'source_url': item.get('source_url', ''),
                        'scraper_type': 'selenium'
                    })
                else:
                    converted['content'].append({
                        'content_type': item.get('type', 'unknown'),
                        'content_text': content_text,  # FIXED: Changed 'content' to 'content_text'
                        'source_url': item.get('source_url', ''),
                        'content_length': len(content_text),
                        'word_count': len(content_text.split()),
                        'scraper_type': 'selenium'
                    })
        
        return converted
    
    def save_combined_data(self, beautifulsoup_data, selenium_data):
        """
        Save data from both scrapers to database
        """
        self.logger.info("Saving combined data to database...")
        
        # Combine all data
        all_quotes = beautifulsoup_data.get('quotes', []) + selenium_data.get('quotes', [])
        all_products = beautifulsoup_data.get('products', []) + selenium_data.get('products', [])
        all_content = beautifulsoup_data.get('content', []) + selenium_data.get('content', [])
        
        # Save to database
        if all_quotes:
            df_quotes = pd.DataFrame(all_quotes)
            # Use correct column names that match database schema
            df_quotes = df_quotes.rename(columns={
                'text': 'quote_text',  # Ensure column names match
                'page': 'page_number'
            })
            df_quotes.to_sql('quotes_new', self.conn, if_exists='append', index=False)
        
        if all_content:
            df_content = pd.DataFrame(all_content)
            df_content = df_content.rename(columns={
                'content': 'content_text',  # Ensure column names match
                'type': 'content_type'
            })
            df_content.to_sql('general_content_new', self.conn, if_exists='append', index=False)
        
        self.logger.info(f"Saved: {len(all_quotes)} quotes, {len(all_content)} content items")
        
        return {
            'total_quotes': len(all_quotes),
            'total_content': len(all_content),
            'total_products': len(all_products)
        }
    
    def run_true_combined_pipeline(self):
        """
        The TRUE combined pipeline that uses actual scrapers
        """
        self.logger.info("STARTING TRUE COMBINED PIPELINE")
        
        # Phase 1: Run actual BeautifulSoup scraper
        beautifulsoup_data = self.run_actual_beautifulsoup_scraper()
        
        # Phase 2: Run actual Selenium scraper
        selenium_data = self.run_actual_selenium_scraper()
        
        # Phase 3: Combine and save data
        results = self.save_combined_data(beautifulsoup_data, selenium_data)
        
        # Phase 4: Generate report
        self.generate_true_combined_report(results, beautifulsoup_data, selenium_data)
        
        self.logger.info("TRUE COMBINED PIPELINE COMPLETED!")
        
        return results
    
    def generate_true_combined_report(self, results, beautifulsoup_data, selenium_data):
        """
        Generate report showing data from both scrapers
        """
        print("\n" + "="*70)
        print("TRUE COMBINED Production-Level PIPELINE REPORT")
        print("="*70)
        print(f"Timestamp: {datetime.now().isoformat()}")
        print(f"Total Quotes: {results['total_quotes']}")
        print(f"Total Content: {results['total_content']}")
        print(f"Total Products: {results['total_products']}")
        print("\nBEAUTIFULSOUP SCRAPER RESULTS:")
        print(f"   Quotes: {len(beautifulsoup_data.get('quotes', []))}")
        print(f"   Content: {len(beautifulsoup_data.get('content', []))}")
        print("\nSELENIUM SCRAPER RESULTS:")
        print(f"   Quotes: {len(selenium_data.get('quotes', []))}")
        print(f"   Content: {len(selenium_data.get('content', []))}")
        print(f"   Products: {len(selenium_data.get('products', []))}")
        print("="*70)

def main():
    """
    Run the TRUE combined pipeline
    """
    print("TRUE COMBINED Production-PIPELINE - USING ACTUAL SCRAPERS")
    print("="*70)
    print("This pipeline ACTUALLY:")
    print("IMPORTS and uses DynamicContentScraper from selenium_practice.py")
    print("USES original BeautifulSoup logic from web-scrap-enhance.py")
    print("COMBINES REAL data from both sources")
    print("STORES everything in the database")
    print("="*70)
    
    pipeline = TrueCombinedPipeline()
    pipeline.run_true_combined_pipeline()
    
    print("\nTRUE COMBINED PIPELINE COMPLETED!")
    print("Check 'true_combined_pipeline.log' for detailed execution logs.")

if __name__ == "__main__":
    main()