import pandas as pd
import numpy as np
import re
import os

# CRITICAL FIX: Force matplotlib to use non-GUI backend
import matplotlib
matplotlib.use('Agg')  # This completely avoids Tcl/Tk
import matplotlib.pyplot as plt
import seaborn as sns

# Display more rows and columns in output
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

def load_and_explore_data(filename='scraped_data.csv'):
    """
    Load the scraped data and perform initial exploration
    """
    print("Loading and exploring the data...")
    
    # Load the CSV file
    df = pd.read_csv(filename)
    
    print("Data loaded successfully!")
    print(f" Dataset shape: {df.shape}")
    print("\n First 10 rows:")
    print(df.head(10))
    
    print("\n Column information:")
    print(df.info())
    
    print("\n Basic statistics:")
    print(df.describe(include='all'))
    
    print("\n Missing values:")
    print(df.isnull().sum())
    
    return df

# Let's run the exploration
df = load_and_explore_data()

#data cleaning functions
def clean_dataframe(df):
    """
    Main data cleaning function that applies various cleaning operations
    """
    print("\n Starting data cleaning process...")
    
    # Create a copy to avoid modifying original data
    cleaned_df = df.copy()
    
    # 1. Remove completely empty rows
    initial_rows = len(cleaned_df)
    cleaned_df = cleaned_df.dropna(how='all')
    print(f"Removed {initial_rows - len(cleaned_df)} completely empty rows")
    
    # 2. Clean the 'Data Type' column
    cleaned_df = clean_data_type_column(cleaned_df)
    
    # 3. Clean the 'Content' column
    cleaned_df = clean_content_column(cleaned_df)
    
    # 4. Handle missing values
    cleaned_df = handle_missing_values(cleaned_df)
    
    # 5. Remove duplicates
    cleaned_df = remove_duplicates(cleaned_df)
    
    # 6. Extract additional features
    cleaned_df = extract_features(cleaned_df)
    
    print(" Data cleaning completed!")
    return cleaned_df

def clean_data_type_column(df):
    """Clean and standardize the Data Type column"""
    print(" Cleaning Data Type column...")
    
    # Remove extra whitespace and standardize case
    df['Data Type'] = df['Data Type'].str.strip().str.title()
    
    # Fill missing data types based on content
    mask = df['Data Type'].isna()
    df.loc[mask, 'Data Type'] = 'Unknown'
    
    print(f" Unique data types: {df['Data Type'].unique()}")
    return df

def clean_content_column(df):
    """Clean the Content column"""
    print(" Cleaning Content column...")
    
    # Remove extra whitespace
    df['Content'] = df['Content'].str.strip()
    
    # Remove special characters but keep basic punctuation
    df['Content'] = df['Content'].apply(lambda x: re.sub(r'[^\w\s\.\,\!\?\-\>]', '', str(x)) if pd.notna(x) else x)
    
    # Replace multiple spaces with single space
    df['Content'] = df['Content'].str.replace(r'\s+', ' ', regex=True)
    
    return df

def handle_missing_values(df):
    """Handle missing values appropriately"""
    print("Handling missing values...")
    
    # For missing Content in known Data Types, fill with appropriate placeholder
    for data_type in df['Data Type'].unique():
        mask = (df['Data Type'] == data_type) & (df['Content'].isna())
        df.loc[mask, 'Content'] = f'No {data_type} content available'
    
    # For completely missing rows in Content
    df['Content'] = df['Content'].fillna('Missing Content')
    
    return df

def remove_duplicates(df):
    """Remove duplicate entries"""
    print("Removing duplicates...")
    
    initial_count = len(df)
    df = df.drop_duplicates(subset=['Data Type', 'Content'], keep='first')
    final_count = len(df)
    
    print(f" Removed {initial_count - final_count} duplicate rows")
    return df

def extract_features(df):
    """Extract additional features from the content"""
    print(" Extracting additional features...")
    
    # Content length
    df['Content_Length'] = df['Content'].str.len()
    
    # Word count
    df['Word_Count'] = df['Content'].str.split().str.len()
    
    # Check if content contains URLs
    df['Contains_URL'] = df['Content'].str.contains('http|www|\\.com|\\.org', case=False, na=False)
    
    # Check if content contains arrows (for links)
    df['Contains_Arrow'] = df['Content'].str.contains('->', na=False)
    
    # Extract language (simple detection - you can enhance this)
    df['Language'] = 'Unknown'
    
    print(" Feature extraction completed")
    return df

#Data Analysis and Visualization
def analyze_data(df):
    """
    Perform data analysis and generate insights
    """
    print("\n Performing data analysis...")
    
    # 1. Basic statistics by data type
    print("\n Statistics by Data Type:")
    stats_by_type = df.groupby('Data Type').agg({
        'Content_Length': ['mean', 'min', 'max'],
        'Word_Count': ['mean', 'min', 'max'],
        'Contains_URL': 'mean'
    }).round(2)
    print(stats_by_type)
    
    # 2. Data type distribution
    print("\n Data Type Distribution:")
    type_distribution = df['Data Type'].value_counts()
    print(type_distribution)
    
    return type_distribution

def visualize_data(df, type_distribution):
    """
    Create visualizations for the cleaned data
    """
    print("\n Creating visualizations...")
    
    # Set up the plotting style
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    
    # 1. Data Type Distribution Pie Chart
    axes[0, 0].pie(type_distribution.values, labels=type_distribution.index, autopct='%1.1f%%')
    axes[0, 0].set_title('Data Type Distribution')
    
    # 2. Content Length Distribution
    axes[0, 1].hist(df['Content_Length'].dropna(), bins=20, edgecolor='black', alpha=0.7)
    axes[0, 1].set_title('Content Length Distribution')
    axes[0, 1].set_xlabel('Content Length (characters)')
    axes[0, 1].set_ylabel('Frequency')
    
    # 3. Word Count by Data Type
    df.boxplot(column='Word_Count', by='Data Type', ax=axes[1, 0])
    axes[1, 0].set_title('Word Count by Data Type')
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    # 4. URL Presence by Data Type
    url_presence = df.groupby('Data Type')['Contains_URL'].mean()
    url_presence.plot(kind='bar', ax=axes[1, 1])
    axes[1, 1].set_title('URL Presence by Data Type')
    axes[1, 1].set_ylabel('Proportion with URLs')
    axes[1, 1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    # Save the figure (this works without Tcl/Tk)
    plt.savefig('data_analysis_visualization.png', dpi=300, bbox_inches='tight')
    print("✅ Visualizations saved as 'data_analysis_visualization.png'")
    
    # Close to free memory
    plt.close()

    #function to export the cleaned data
    """
    #Export the cleaned data to a new CSV file
    """
def export_cleaned_data(df, filename='cleaned_scraped_data.csv'):
       print(f"\n Exporting cleaned data to {filename}...")
    
       # Export main cleaned data
       df.to_csv(filename, index=False)
    
       # Also create a summary report
       create_summary_report(df, 'data_cleaning_summary.txt')
    
       print("Cleaned data exported successfully!")

def create_summary_report(df, filename):
    """Create a comprehensive summary report"""
    with open(filename, 'w') as f:
        f.write("DATA CLEANING SUMMARY REPORT\n")
        f.write("=" * 50 + "\n\n")
        
        f.write(f"Total Rows: {len(df)}\n")
        f.write(f"Total Columns: {len(df.columns)}\n\n")
        
        f.write("DATA TYPES DISTRIBUTION:\n")
        f.write("-" * 30 + "\n")
        for data_type, count in df['Data Type'].value_counts().items():
            f.write(f"{data_type}: {count} rows\n")
        
        f.write(f"\nCONTENT STATISTICS:\n")
        f.write("-" * 30 + "\n")
        f.write(f"Average Content Length: {df['Content_Length'].mean():.2f} characters\n")
        f.write(f"Average Word Count: {df['Word_Count'].mean():.2f} words\n")
        f.write(f"Rows containing URLs: {df['Contains_URL'].sum()}\n")
        f.write(f"Rows containing arrows: {df['Contains_Arrow'].sum()}\n")
        
        f.write(f"\nSAMPLE OF CLEANED DATA:\n")
        f.write("-" * 30 + "\n")
        f.write(df.head(10).to_string())

    #Main Execution
def main():
    #Main function to run the entire data cleaning pipeline
    print(" Starting Data Cleaning Pipeline")
    print("=" * 50)
    
    try:
        # Step 1: Load and explore data
        df = load_and_explore_data('scraped_data.csv')
        
        # Step 2: Clean the data
        cleaned_df = clean_dataframe(df)
        
        # Step 3: Analyze the data
        type_distribution = analyze_data(cleaned_df)
        
        # Step 4: Visualize the data
        visualize_data(cleaned_df, type_distribution)
        
        # Step 5: Export cleaned data
        export_cleaned_data(cleaned_df)
        
        # Display final cleaned data sample
        print("\n FINAL CLEANED DATA SAMPLE:")
        print("=" * 40)
        print(cleaned_df.head(15))
        
        print("\n Data cleaning pipeline completed successfully!")
        
    except FileNotFoundError:
        print("Error: scraped_data.csv file not found. Please run your web scraper first.")
    except Exception as e:
        print(f" An error occurred: {e}")

if __name__ == "__main__":
    main()