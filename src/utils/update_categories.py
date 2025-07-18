import polars as pl
import httpx
import os

def main():
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Fetch all datasets
    base_url = 'https://api.dane.gov.pl/1.4'
    all_datasets = []
    page = 1
    
    while True:
        try:
            response = httpx.get(f"{base_url}/datasets", params={'page': page, 'per_page': 100})
            if response.status_code != 200:
                break
            data = response.json()
        except:
            break
            
        datasets = data.get('data', [])
        if not datasets:
            break
            
        for dataset in datasets:
            attributes = dataset.get('attributes', {})
            all_datasets.append({
                'id': dataset.get('id'),
                'categories': attributes.get('categories'),
                'category': attributes.get('category'),
            })
        
        if 'next' not in data.get('links', {}):
            break
        page += 1
    
    # Process categories (list)
    categories_data = []
    for dataset in all_datasets:
        if dataset.get('categories'):
            for cat in dataset['categories']:
                if isinstance(cat, dict):
                    categories_data.append({
                        'category_id': cat.get('id'),
                        'category_title': cat.get('title'),
                    })
    
    # Process category (single)
    category_data = []
    for dataset in all_datasets:
        if dataset.get('category') and isinstance(dataset['category'], dict):
            cat = dataset['category']
            category_data.append({
                'category_id': cat.get('id'),
                'category_title': cat.get('title')
            })
    
    # Create unique CSVs
    if categories_data:
        df = pl.DataFrame(categories_data)
        unique_categories = df.group_by(['category_id', 'category_title']).agg(pl.len().alias('usage_count'))
        unique_categories = unique_categories.sort('usage_count', descending=True)
        unique_categories.write_csv('data/categories_unique.csv')
    
    if category_data:
        df = pl.DataFrame(category_data)
        unique_category = df.group_by(['category_id', 'category_title']).agg(pl.len().alias('usage_count'))
        unique_category = unique_category.sort('usage_count', descending=True)
        unique_category.write_csv('data/category_unique.csv')

if __name__ == "__main__":
    main() 