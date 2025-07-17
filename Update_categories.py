import pandas as pd
import os
import requests
from collections import defaultdict

def fetch_all_datasets():
    """
    Fetch all datasets from dane.gov.pl API
    """
    print("=== FETCHING DATASETS FROM API ===")
    
    base_url = 'https://api.dane.gov.pl/1.4'
    params = {
        'page': 1,
        'per_page': 100,
        'sort': 'id'
    }

    all_datasets = []
    while True:
        print(f"Fetching page {params['page']}...")
        response = requests.get(base_url + '/datasets', params=params)
        if response.status_code == 200:
            try:
                data = response.json()
                datasets = data.get('data')
                for dataset in datasets:
                    attributes = dataset.get('attributes', {})
                    all_datasets.append(
                        {
                            'id': dataset.get('id'),
                            'title': attributes.get('title'),
                            'formats': attributes.get('formats'),
                            'license_name': attributes.get('license_name'),
                            'type': dataset.get('type'),
                            'categories': attributes.get('categories'),
                            'category': attributes.get('category'),
                        }
                    )
                if 'next' in data.get('links'):
                    params['page'] += 1
                else:
                    break
            except Exception as e:
                print(f'Error: {e}')
                break
        else:
            print(f'Failed to fetch data {params}. Status code: {response.status_code}')
            break
    
    print(f"✅ Fetched {len(all_datasets)} datasets from API\n")
    return all_datasets

def create_categories_csvs(all_datasets):
    """
    Create unique category CSV files from all_datasets:
    1. categories_unique.csv - unique categories from 'categories' field (list of categories)
    2. category_unique.csv - unique categories from 'category' field (single category)
    """
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    print("=== PROCESSING CATEGORIES DATA ===\n")
    
    # Lists to store processed data
    categories_data = []  # For 'categories' field
    category_data = []    # For 'category' field
    
    # Track usage counts
    categories_usage = defaultdict(int)
    category_usage = defaultdict(int)
    
    # Process each dataset
    for dataset in all_datasets:
        dataset_id = dataset.get('id')
        dataset_title = dataset.get('title')
        
        # Process 'categories' field (list)
        if dataset.get('categories'):
            categories = dataset['categories']
            if isinstance(categories, list):
                for cat in categories:
                    if isinstance(cat, dict):
                        cat_id = cat.get('id')
                        cat_title = cat.get('title')
                        cat_code = cat.get('code')
                        
                        categories_data.append({
                            'dataset_id': dataset_id,
                            'dataset_title': dataset_title,
                            'category_id': cat_id,
                            'category_title': cat_title,
                            'category_code': cat_code
                        })
                        
                        if cat_id:
                            categories_usage[cat_id] += 1
        
        # Process 'category' field (single)
        if dataset.get('category'):
            category = dataset['category']
            if isinstance(category, dict):
                cat_id = category.get('id')
                cat_title = category.get('title')
                
                category_data.append({
                    'dataset_id': dataset_id,
                    'dataset_title': dataset_title,
                    'category_id': cat_id,
                    'category_title': cat_title
                })
                
                if cat_id:
                    category_usage[cat_id] += 1
    
    # Create DataFrames
    categories_df = pd.DataFrame(categories_data)
    category_df = pd.DataFrame(category_data)
    
    # Print statistics
    print(f"Categories data (from 'categories' field):")
    print(f"  Total records: {len(categories_df)}")
    print(f"  Unique categories: {categories_df['category_id'].nunique() if not categories_df.empty else 0}")
    print(f"  Datasets with categories: {categories_df['dataset_id'].nunique() if not categories_df.empty else 0}")
    
    print(f"\nCategory data (from 'category' field):")
    print(f"  Total records: {len(category_df)}")
    print(f"  Unique categories: {category_df['category_id'].nunique() if not category_df.empty else 0}")
    print(f"  Datasets with category: {category_df['dataset_id'].nunique() if not category_df.empty else 0}")
    
    # Save only unique category files
    if not categories_df.empty:
        # Create unique categories summary
        unique_categories = categories_df.groupby(['category_id', 'category_title', 'category_code']).size().reset_index(name='usage_count')
        unique_categories = unique_categories.sort_values('usage_count', ascending=False)
        unique_categories.to_csv('data/categories_unique.csv', index=False, encoding='utf-8')
        print(f"\n✅ Created: data/categories_unique.csv ({len(unique_categories)} unique categories)")
    else:
        print("⚠️  No categories data found - skipping categories_unique.csv")
    
    if not category_df.empty:
        # Create unique category summary
        unique_category = category_df.groupby(['category_id', 'category_title']).size().reset_index(name='usage_count')
        unique_category = unique_category.sort_values('usage_count', ascending=False)
        unique_category.to_csv('data/category_unique.csv', index=False, encoding='utf-8')
        print(f"✅ Created: data/category_unique.csv ({len(unique_category)} unique categories)")
    else:
        print("⚠️  No category data found - skipping category_unique.csv")
    
    # Show sample data
    if not categories_df.empty and 'unique_categories' in locals():
        print(f"\n=== TOP 10 MOST USED CATEGORIES (from 'categories' field) ===")
        print(unique_categories.head(10).to_string(index=False))
    
    if not category_df.empty and 'unique_category' in locals():
        print(f"\n=== TOP 10 MOST USED SINGLE CATEGORIES (from 'category' field) ===")
        print(unique_category.head(10).to_string(index=False))
    
    return categories_df, category_df

if __name__ == "__main__":
    try:
        # Fetch all datasets from API
        all_datasets = fetch_all_datasets()
        
        # Create categories CSV files
        categories_df, category_df = create_categories_csvs(all_datasets)
        
        print("\n🎉 Categories CSV files created successfully!")
        print(f"📁 Check the 'data/' folder for your CSV files")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc() 