import requests
from bs4 import BeautifulSoup
import csv
import time

def enhanced_scraper():
    """
    Enhanced web scraper with better error handling and data processing
    """
    # Target URL
    url = "https://www.passiton.com/inspirational-quotes/"
    
    # Headers to mimic a real browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        print("Starting web scraping...")
        
        # Make the request with headers
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        print("Successfully connected to the website")
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'lxml')  # Using lxml parser for better performance
        
        # Let's try to extract different types of data
        scraped_data = {}
        
        # Extract page title
        page_title = soup.find('title')
        if page_title:
            scraped_data['page_title'] = page_title.get_text().strip()
            print(f"Page Title: {scraped_data['page_title']}")
        
        # Extract all links
        links = soup.find_all('a', href=True)
        scraped_data['links'] = []
        
        for link in links[:15]:  # Get first 15 links
            link_data = {
                'text': link.get_text().strip()[:50],  # Limit text length
                'url': link['href']
            }
            scraped_data['links'].append(link_data)
        
        # Extract paragraphs
        paragraphs = soup.find_all('p')
        scraped_data['paragraphs'] = []
        
        for para in paragraphs[:5]:  # Get first 5 paragraphs
            text = para.get_text().strip()
            if text and len(text) > 10:  # Only meaningful paragraphs
                scraped_data['paragraphs'].append(text[:200])  # Limit length
        
        # Display results
        print("\n Scraping Results:")
        print(f"• Found {len(scraped_data['links'])} links")
        print(f"• Found {len(scraped_data['paragraphs'])} paragraphs")
        
        # Print some sample links
        print("\n🔗 Sample Links:")
        for i, link in enumerate(scraped_data['links'][:5], 1):
            print(f"  {i}. {link['text']} -> {link['url'][:50]}...")
        
        return scraped_data
        
    except requests.exceptions.HTTPError as e:
        print(f" HTTP Error: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f" Connection Error: {e}")
    except requests.exceptions.Timeout as e:
        print(f" Timeout Error: {e}")
    except Exception as e:
        print(f" Unexpected error: {e}")
    
    return {}

def save_to_csv(data, filename='scraped_data.csv'):
    """
    Save scraped data to CSV file
    """
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            # Write header
            writer.writerow(['Data Type', 'Content'])
            
            # Write page title
            if 'page_title' in data:
                writer.writerow(['Page Title', data['page_title']])
            
            # Write links
            for link in data.get('links', []):
                writer.writerow(['Link', f"{link['text']} -> {link['url']}"])
            
            # Write paragraphs
            for i, para in enumerate(data.get('paragraphs', [])):
                writer.writerow([f'Paragraph {i+1}', para])
        
        print(f"Data saved to {filename}")
        
    except Exception as e:
        print(f"Error saving to CSV: {e}")

if __name__ == "__main__":
    # Add a small delay to be respectful to the server
    time.sleep(2)
    
    # Run the enhanced scraper
    scraped_data = enhanced_scraper()
    
    # Save data to CSV
    if scraped_data:
        save_to_csv(scraped_data)
        print("Web scraping completed successfully..!")
    else:
        print("No data was scraped.")