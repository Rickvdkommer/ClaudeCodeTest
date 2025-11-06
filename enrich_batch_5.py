#!/usr/bin/env python3
"""
Enrichment script for batch 5 - matches leads to Golden Sheet data
"""
import json
import csv
import os
from difflib import SequenceMatcher

def similarity(a, b):
    """Calculate similarity ratio between two strings"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def load_json_batch(filepath):
    """Load the batch JSON file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_categories(filepath):
    """Load category count CSV"""
    categories = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            categories[row['Primary Category']] = int(row['Number of Assets Tested'])
    return categories

def load_pivot_table(filepath):
    """Load pivot table with brand data"""
    brands = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        # Use csv.reader to handle quoted fields properly
        reader = csv.reader(f)
        rows = list(reader)

    # Skip first 3 rows (headers and metadata)
    for row in rows[3:]:
        if len(row) < 3:
            continue

        # Column structure: [index, COUNTA, Main Brand, amazon_prime, instagram, netflix, standalone, tiktok, youtube_shorts, Grand Total, platforms_list, markets_list]
        brand_name = row[1].strip() if len(row) > 1 else ''

        # Skip empty brands or header rows
        if not brand_name or brand_name == 'Main Brand' or brand_name == 'MAIN BRAND':
            continue

        # Extract Grand Total (column 8)
        grand_total = row[8].strip() if len(row) > 8 else '0'

        # Extract platform columns (indices 2-7)
        platform_names = ['amazon_prime', 'instagram', 'netflix', 'standalone', 'tiktok', 'youtube_shorts']
        platforms = []
        for idx, platform in enumerate(platform_names):
            col_idx = 2 + idx
            if len(row) > col_idx and row[col_idx].strip() and row[col_idx].strip() not in ['', '0']:
                platforms.append(platform)

        # Extract platforms list (column 9) and markets (column 10)
        platforms_str = row[9].strip() if len(row) > 9 else ''
        markets = row[10].strip() if len(row) > 10 else ''

        brands[brand_name] = {
            'total_assets': grand_total,
            'platforms': platforms_str if platforms_str else ', '.join(platforms),
            'markets': markets
        }

    return brands

def match_brand(company_name, brands):
    """Find the best matching brand using fuzzy matching"""
    if not company_name:
        return None, 0.0

    # First try exact match
    if company_name in brands:
        return company_name, 1.0

    # Handle common variations - map company names to their brand equivalents
    company_to_brand_map = {
        'Amazon Business': 'Amazon',
        'Prime Video & Amazon MGM Studios': 'Amazon',  # Note: actual brand is separate but maps to Amazon
        'Ford Motor Company': 'Ford',
        'Samsung Electronics': 'Samsung',
        'The Coca-Cola Company': 'Coca-Cola',
        'The Walt Disney Company': 'Disney',
        'Metro by T-Mobile': 'T-Mobile',
    }

    # Check if company is in our mapping
    if company_name in company_to_brand_map:
        target_brand = company_to_brand_map[company_name]
        if target_brand in brands:
            return target_brand, 0.95

    # Fuzzy matching for other cases
    best_match = None
    best_score = 0.0

    for brand in brands.keys():
        score = similarity(company_name, brand)
        if score > best_score:
            best_score = score
            best_match = brand

    # Only return if similarity is high enough (>= 0.7)
    if best_score >= 0.7:
        return best_match, best_score

    return None, 0.0

def categorize_company(company_name, industry):
    """Categorize company into one of 29 categories"""

    # Define category mapping rules
    category_rules = {
        'Automotive': ['Ford Motor Company', 'Jeep', 'A2MAC1 - Decode the future'],
        'Consumer Electronics': ['Apple', 'Samsung Electronics'],
        'Retail and E-Commerce': ['Amazon', 'Amazon Business', 'Sainsbury\'s', 'Pets at Home', 'PetSmart', 'Nike'],
        'Entertainment and Streaming': ['The Walt Disney Company', 'DIRECTV', 'Prime Video & Amazon MGM Studios'],
        'Telecommunications': ['T-Mobile'],
        'Food and Beverage': ['The Coca-Cola Company', 'Burger King'],
        'Hospital & Health Care': ['Philips'],
        'Software': ['A2MAC1 - Decode the future'],
        'Financial Services': [],
    }

    # Industry-based categorization
    industry_map = {
        'Automotive': 'Automotive',
        'Consumer Electronics': 'Electronics and Technology',
        'Computer Software': 'Software',
        'Entertainment': 'Entertainment and Streaming',
        'Telecommunications': 'Telecommunications',
        'Food & Beverages': 'Food and Beverage',
        'Retail': 'Retail and E-Commerce',
        'Hospital & Health Care': 'Health, Wellness, and Fitness',
        'Restaurants': 'QSR (Quick Service Restaurants)',
    }

    # Check industry first
    if industry in industry_map:
        return industry_map[industry]

    # Check company name against rules
    for category, companies in category_rules.items():
        if company_name in companies:
            return category

    # Default fallbacks based on keywords
    company_lower = company_name.lower() if company_name else ''

    if any(word in company_lower for word in ['motor', 'automotive', 'car', 'ford', 'jeep']):
        return 'Automotive'
    elif any(word in company_lower for word in ['electronics', 'samsung', 'apple', 'tech']):
        return 'Electronics and Technology'
    elif any(word in company_lower for word in ['retail', 'store', 'shop', 'amazon', 'market']):
        return 'Retail and E-Commerce'
    elif any(word in company_lower for word in ['entertainment', 'disney', 'streaming', 'video', 'tv']):
        return 'Entertainment and Streaming'
    elif any(word in company_lower for word in ['mobile', 't-mobile', 'telecom', 'wireless']):
        return 'Telecommunications'
    elif any(word in company_lower for word in ['food', 'beverage', 'coca-cola', 'restaurant', 'burger']):
        return 'Food and Beverage'
    elif any(word in company_lower for word in ['software', 'tech', 'saas']):
        return 'Software'
    elif any(word in company_lower for word in ['health', 'medical', 'hospital', 'care', 'philips']):
        return 'Health, Wellness, and Fitness'
    elif any(word in company_lower for word in ['pet', 'animal']):
        return 'Pet Food & Care'
    elif any(word in company_lower for word in ['finance', 'bank', 'financial']):
        return 'Finance and Banking'
    else:
        # Default category for unmatched
        return 'Services (Professional and Consumer)'

def enrich_leads(batch_data, brands, categories):
    """Enrich all leads with Golden Sheet data"""
    enriched_leads = []

    for lead in batch_data['leads']:
        # Extract company name
        company_name = lead.get('inline-flex', '')
        industry = lead.get('font-qanelas 13', '')

        # Match to brand in pivot table
        matched_brand, match_score = match_brand(company_name, brands)

        if matched_brand:
            brand_data = brands[matched_brand]
            enriched_lead = {
                **lead,  # Keep all original fields
                'brand_in_golden_sheet': 'Yes',
                'total_assets_tested': brand_data['total_assets'],
                'platforms_tested': brand_data['platforms'],
                'markets_tested': brand_data['markets'],
                'matched_brand_name': matched_brand,
                'match_confidence': f'{match_score:.2f}'
            }
        else:
            enriched_lead = {
                **lead,
                'brand_in_golden_sheet': 'No',
                'total_assets_tested': '',
                'platforms_tested': '',
                'markets_tested': '',
                'matched_brand_name': '',
                'match_confidence': '0.00'
            }

        # Categorize company
        category = categorize_company(company_name, industry)
        category_count = categories.get(category, 0)

        enriched_lead['company_category'] = category
        enriched_lead['category_asset_count'] = category_count

        enriched_leads.append(enriched_lead)

    return enriched_leads

def write_enriched_csv(enriched_leads, output_path):
    """Write enriched leads to CSV"""
    if not enriched_leads:
        print("No leads to write")
        return

    # Get all unique field names
    fieldnames = list(enriched_leads[0].keys())

    # Ensure new enrichment columns are at the end
    enrichment_cols = ['brand_in_golden_sheet', 'total_assets_tested', 'platforms_tested',
                       'markets_tested', 'matched_brand_name', 'match_confidence',
                       'company_category', 'category_asset_count']

    # Reorder: original columns first, then enrichment columns
    original_cols = [f for f in fieldnames if f not in enrichment_cols]
    fieldnames = original_cols + enrichment_cols

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_leads)

def main():
    # File paths
    batch_file = '/home/user/ClaudeCodeTest/agent_batches/enrichment_batch_5.json'
    categories_file = '/home/user/ClaudeCodeTest/Golden Sheet - Category_Count.csv'
    pivot_file = '/home/user/ClaudeCodeTest/Golden Sheet - Pivot Table Brands.csv'
    output_file = '/home/user/ClaudeCodeTest/enriched_results/enriched_batch_5.csv'

    print("Loading input files...")
    batch_data = load_json_batch(batch_file)
    categories = load_categories(categories_file)
    brands = load_pivot_table(pivot_file)

    print(f"Loaded {len(batch_data['leads'])} leads")
    print(f"Loaded {len(categories)} categories")
    print(f"Loaded {len(brands)} brands")

    print("\nEnriching leads...")
    enriched_leads = enrich_leads(batch_data, brands, categories)

    print(f"\nWriting enriched data to {output_file}...")
    write_enriched_csv(enriched_leads, output_file)

    # Generate summary statistics
    matched_count = sum(1 for lead in enriched_leads if lead['brand_in_golden_sheet'] == 'Yes')

    print("\n" + "="*60)
    print("ENRICHMENT SUMMARY")
    print("="*60)
    print(f"Total leads processed: {len(enriched_leads)}")
    print(f"Brands matched to Golden Sheet: {matched_count}")
    print(f"Brands not matched: {len(enriched_leads) - matched_count}")
    print(f"Match rate: {matched_count/len(enriched_leads)*100:.1f}%")

    # Top 3 companies with enrichment data
    print("\n" + "="*60)
    print("TOP 3 COMPANIES WITH ENRICHMENT DATA")
    print("="*60)

    matched_leads = [l for l in enriched_leads if l['brand_in_golden_sheet'] == 'Yes']
    # Sort by total assets
    matched_leads_sorted = sorted(matched_leads,
                                   key=lambda x: int(x['total_assets_tested']) if x['total_assets_tested'] else 0,
                                   reverse=True)

    for i, lead in enumerate(matched_leads_sorted[:3], 1):
        print(f"\n{i}. {lead['inline-flex']}")
        print(f"   - Matched Brand: {lead['matched_brand_name']}")
        print(f"   - Total Assets Tested: {lead['total_assets_tested']}")
        print(f"   - Platforms: {lead['platforms_tested']}")
        print(f"   - Markets: {lead['markets_tested']}")
        print(f"   - Category: {lead['company_category']} ({lead['category_asset_count']} assets)")
        print(f"   - Match Confidence: {lead['match_confidence']}")

    print("\n" + "="*60)
    print(f"Output saved to: {output_file}")
    print("="*60)

if __name__ == '__main__':
    main()
