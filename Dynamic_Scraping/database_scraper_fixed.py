import sqlite3
import pandas as pd
import json
from datetime import datetime
import logging

class ScrapingDatabase:
    """
    Database manager for storing scraped data
    This replaces CSV files with a proper database
    """
    
    def __init__(self, db_name='scraping_data.db'):
        self.db_name = db_name
        self.setup_logging()  # FIXED: Setup logging FIRST
        self.setup_database() # Then setup database
    
    def setup_logging(self):
        """Setup logging for database operations"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraping_database.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logger setup completed!")
    
    def setup_database(self):
        """
        Create database tables with proper schema
        This is where we define our data structure
        """
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            # Table for quotes data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    quote_text TEXT NOT NULL,
                    author TEXT NOT NULL,
                    tags TEXT,
                    page_number INTEGER,
                    source_url TEXT,
                    scrape_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    data_quality_score INTEGER DEFAULT 100
                )
            ''')
            
            # Table for product data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_name TEXT NOT NULL,
                    price TEXT,
                    description TEXT,
                    category TEXT,
                    source_url TEXT,
                    scrape_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_available BOOLEAN DEFAULT TRUE
                )
            ''')
            
            # Table for general content (paragraphs, headings, etc.)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS general_content (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content_type TEXT NOT NULL,
                    content_text TEXT NOT NULL,
                    source_url TEXT,
                    content_length INTEGER,
                    word_count INTEGER,
                    scrape_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Table for tracking scraping sessions
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scraping_sessions (
                    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    end_time DATETIME,
                    total_records INTEGER,
                    status TEXT DEFAULT 'running',
                    websites_scraped TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            self.logger.info("Database tables created successfully!")
            
        except Exception as e:
            self.logger.error(f"Error setting up database: {e}")
    
    def start_scraping_session(self):
        """Start a new scraping session and return session ID"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO scraping_sessions (start_time, status) 
                VALUES (datetime('now'), 'running')
            ''')
            
            session_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            self.logger.info(f"Started scraping session ID: {session_id}")
            return session_id
            
        except Exception as e:
            self.logger.error(f"Error starting session: {e}")
            return None
    
    def end_scraping_session(self, session_id, total_records, websites):
        """Mark a scraping session as completed"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE scraping_sessions 
                SET end_time = datetime('now'), 
                    total_records = ?,
                    status = 'completed',
                    websites_scraped = ?
                WHERE session_id = ?
            ''', (total_records, websites, session_id))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Completed scraping session ID: {session_id}")
            
        except Exception as e:
            self.logger.error(f"Error ending session: {e}")
    
    def save_quotes(self, quotes_data):
        """
        Save quotes data to database
        quotes_data should be a list of dictionaries
        """
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            for quote in quotes_data:
                # Calculate data quality score
                quality_score = self.calculate_quality_score(quote)
                
                cursor.execute('''
                    INSERT INTO quotes 
                    (quote_text, author, tags, page_number, source_url, data_quality_score)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    quote.get('text', ''),
                    quote.get('author', 'Unknown'),
                    quote.get('tags', ''),
                    quote.get('page', 1),
                    quote.get('source_url', ''),
                    quality_score
                ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Saved {len(quotes_data)} quotes to database")
            
        except Exception as e:
            self.logger.error(f"Error saving quotes: {e}")
    
    def save_products(self, products_data):
        """
        Save products data to database
        """
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            for product in products_data:
                cursor.execute('''
                    INSERT INTO products 
                    (product_name, price, description, category, source_url)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    product.get('name', 'Unknown Product'),
                    product.get('price', 'N/A'),
                    product.get('description', ''),
                    product.get('category', 'General'),
                    product.get('source_url', '')
                ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Saved {len(products_data)} products to database")
            
        except Exception as e:
            self.logger.error(f"Error saving products: {e}")
    
    def save_general_content(self, content_data):
        """
        Save general content to database
        """
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            for content in content_data:
                cursor.execute('''
                    INSERT INTO general_content 
                    (content_type, content_text, source_url, content_length, word_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    content.get('type', 'unknown'),
                    content.get('content', ''),
                    content.get('source_url', ''),
                    len(content.get('content', '')),
                    len(content.get('content', '').split())
                ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Saved {len(content_data)} content items to database")
            
        except Exception as e:
            self.logger.error(f"Error saving content: {e}")
    
    def calculate_quality_score(self, data):
        """
        Calculate data quality score (0-100)
        Higher score = better quality data
        """
        score = 100
        
        # Penalize for missing author
        if not data.get('author') or data.get('author') == 'Unknown':
            score -= 30
        
        # Penalize for short quotes
        if len(data.get('text', '')) < 20:
            score -= 20
        
        # Penalize for missing tags
        if not data.get('tags'):
            score -= 10
        
        return max(0, score)  # Ensure score doesn't go below 0
    
    def get_scraping_stats(self):
        """Get statistics about scraped data"""
        try:
            conn = sqlite3.connect(self.db_name)
            
            stats = {
                'total_quotes': pd.read_sql('SELECT COUNT(*) as count FROM quotes', conn).iloc[0]['count'],
                'total_products': pd.read_sql('SELECT COUNT(*) as count FROM products', conn).iloc[0]['count'],
                'total_content': pd.read_sql('SELECT COUNT(*) as count FROM general_content', conn).iloc[0]['count'],
                'top_authors': pd.read_sql('SELECT author, COUNT(*) as count FROM quotes GROUP BY author ORDER BY count DESC LIMIT 5', conn),
                'recent_sessions': pd.read_sql('SELECT * FROM scraping_sessions ORDER BY start_time DESC LIMIT 3', conn)
            }
            
            conn.close()
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting stats: {e}")
            return {}
    
    def export_to_csv(self, table_name, filename=None):
        """Export any table to CSV for backup or analysis"""
        try:
            if not filename:
                filename = f"{table_name}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            conn = sqlite3.connect(self.db_name)
            df = pd.read_sql(f'SELECT * FROM {table_name}', conn)
            df.to_csv(filename, index=False)
            conn.close()
            
            self.logger.info(f"Exported {len(df)} records from {table_name} to {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Error exporting {table_name}: {e}")
            return None

# Test the database functionality
def test_database_functionality():
    """
    Test all database functions to make sure everything works
    """
    print("🧪 Testing Database Functionality...")
    
    # Initialize database
    db = ScrapingDatabase()
    
    # Test data
    test_quotes = [
        {
            'text': 'The only way to do great work is to love what you do.',
            'author': 'Steve Jobs',
            'tags': 'work, passion, greatness',
            'page': 1,
            'source_url': 'https://quotes.toscrape.com/'
        },
        {
            'text': 'Life is what happens when you are busy making other plans.',
            'author': 'John Lennon', 
            'tags': 'life, plans',
            'page': 2,
            'source_url': 'https://quotes.toscrape.com/'
        }
    ]
    
    test_products = [
        {
            'name': 'Wireless Mouse',
            'price': '$29.99',
            'description': 'Ergonomic wireless mouse with high precision',
            'category': 'Electronics',
            'source_url': 'https://webscraper.io/test-sites/e-commerce/allinone'
        },
        {
            'name': 'Mechanical Keyboard',
            'price': '$89.99', 
            'description': 'RGB mechanical keyboard with blue switches',
            'category': 'Electronics',
            'source_url': 'https://webscraper.io/test-sites/e-commerce/allinone'
        }
    ]
    
    test_content = [
        {
            'type': 'paragraph',
            'content': 'This is a test paragraph for database storage.',
            'source_url': 'https://example.com'
        }
    ]
    
    # Start a scraping session
    session_id = db.start_scraping_session()
    
    if session_id:
        # Save test data
        db.save_quotes(test_quotes)
        db.save_products(test_products)
        db.save_general_content(test_content)
        
        # Get statistics
        stats = db.get_scraping_stats()
        
        print("\n DATABASE STATISTICS:")
        print(f"   Total quotes: {stats['total_quotes']}")
        print(f"   Total products: {stats['total_products']}") 
        print(f"   Total content: {stats['total_content']}")
        
        # Show top authors
        if not stats['top_authors'].empty:
            print(f"\n TOP AUTHORS:")
            for _, row in stats['top_authors'].iterrows():
                print(f"   {row['author']}: {row['count']} quotes")
        
        # Export for backup
        db.export_to_csv('quotes')
        db.export_to_csv('products')
        
        # End session
        total_records = len(test_quotes) + len(test_products) + len(test_content)
        db.end_scraping_session(session_id, total_records, 'test_sites')
        
        print(f"\n✅ Database test completed! Check 'scraping_data.db' file.")
        
        # Show file size
        import os
        if os.path.exists('scraping_data.db'):
            size_kb = os.path.getsize('scraping_data.db') / 1024
            print(f"Database file size: {size_kb:.2f} KB")
    else:
        print("Failed to start scraping session")

if __name__ == "__main__":
    test_database_functionality()