import requests
from bs4 import BeautifulSoup
import csv

def scrape_website():
    # Step 1: Send HTTP request to the website
    url = "https://www.geeksforgeeks.org/python-programming-language/"
    
    try:
        # Send GET request to the website
        response = requests.get(url)
        
        # Check if request was successful
        response.raise_for_status()  # This will raise an exception for bad status codes
        
        # Step 2: Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Step 3: Extract data - let's get article titles
        print("Scraping article titles from GeeksforGeeks...")
        
        # Find all article titles (adjust selector based on actual website structure)
        # Note: Website structure might change, so we might need to adjust selectors
        articles = soup.find_all('h2')  # Common tag for titles
        
        # Extract and print titles
        titles = []
        for index, article in enumerate(articles[:10], 1):  # Get first 10 articles
            title = article.get_text().strip()
            if title:  # Only add non-empty titles
                titles.append(title)
                print(f"{index}. {title}")
        
        return titles
        
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while making the request: {e}")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

if __name__ == "__main__":
    scrape_website()