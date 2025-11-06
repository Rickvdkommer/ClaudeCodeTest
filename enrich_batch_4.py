#!/usr/bin/env python3
import json
import csv
from difflib import SequenceMatcher
import os

# Load input files
with open('/home/user/ClaudeCodeTest/agent_batches/enrichment_batch_4.json', 'r') as f:
    batch_data = json.load(f)

# Load category counts
categories = {}
with open('/home/user/ClaudeCodeTest/Golden Sheet - Category_Count.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        categories[row['Primary Category']] = int(row['Number of Assets Tested'])

# Parse pivot table using csv module
brands_data = {}
with open('/home/user/ClaudeCodeTest/Golden Sheet - Pivot Table Brands.csv', 'r') as f:
    reader = csv.reader(f)
    lines = list(reader)

    for i, row in enumerate(lines):
        if i < 3:  # Skip header rows (first 3 rows)
            continue

        if len(row) >= 9:
            # Column structure: [index, brand, amazon_prime, instagram, netflix, standalone, tiktok, youtube_shorts, grand_total, platforms, markets]
            brand_name = row[1].strip() if len(row) > 1 else ''
            grand_total = row[8].strip() if len(row) > 8 else ''
            platforms = row[9].strip() if len(row) > 9 else ''
            markets = row[10].strip() if len(row) > 10 else ''

            if brand_name and brand_name not in ['', 'MAIN BRAND', 'Grand Total']:
                brands_data[brand_name] = {
                    'grand_total': grand_total,
                    'platforms': platforms,
                    'markets': markets
                }

def fuzzy_match(company, brand, threshold=0.6):
    """Check if company name matches brand with fuzzy matching"""
    company_lower = company.lower().strip()
    brand_lower = brand.lower().strip()

    # Exact match
    if company_lower == brand_lower:
        return True

    # Contains match
    if brand_lower in company_lower or company_lower in brand_lower:
        return True

    # Handle common variations - map brand names to company variations
    brand_to_companies = {
        'amazon': ['amazon', 'amazon music', 'amazon web services', 'aws', 'amazon astro'],
        'apple': ['apple'],
        'nike': ['nike'],
        'disney': ['the walt disney company', 'disney', 'walt disney', 'pixar'],
        'coca-cola': ['the coca-cola company', 'coca cola', 'coca-cola'],
        'bodyarmor': ['bodyarmor'],
        'estée lauder': ['the estée lauder companies', 'estée lauder', 'estee lauder'],
        'clinique': ['clinique'],
        'mac cosmetics': ['mac cosmetics'],
        'ebay': ['ebay', 'ebay.de'],
        'ford': ['ford motor company', 'ford'],
        't-mobile': ['t-mobile'],
        'metro by t-mobile': ['metro by t-mobile'],
        'philips': ['philips'],
        'directv': ['directv'],
        'sharkninja': ['sharkninja'],
        'jeep': ['jeep'],
        'hugo boss': ['hugo boss']
    }

    # Check if this brand has defined variations
    for brand_key, company_variations in brand_to_companies.items():
        if brand_key in brand_lower:
            for variation in company_variations:
                if variation in company_lower:
                    return True

    # Reverse check - if company matches any brand variation
    for brand_key, company_variations in brand_to_companies.items():
        for variation in company_variations:
            if variation in company_lower and brand_key in brand_lower:
                return True

    # Sequence matcher as fallback
    ratio = SequenceMatcher(None, company_lower, brand_lower).ratio()
    return ratio >= threshold

def find_brand_match(company_name):
    """Find matching brand in pivot table"""
    for brand, data in brands_data.items():
        if fuzzy_match(company_name, brand):
            return brand, data
    return None, None

def categorize_company(company_name, industry):
    """Categorize company into one of 29 categories"""
    company_lower = company_name.lower()
    industry_lower = industry.lower() if industry else ''

    # Mapping logic based on company name and industry
    if any(x in company_lower for x in ['apple', 'amazon', 'ebay', 'google']):
        if 'amazon web services' in company_lower or 'aws' in company_lower:
            return 'Software'
        elif any(x in company_lower for x in ['amazon music', 'disney', 'directv']):
            return 'Entertainment and Streaming'
        return 'Electronics and Technology'

    if 'cosmetics' in industry_lower or any(x in company_lower for x in ['lauder', 'clinique', 'mac cosmetics', 'estée', 'estee']):
        return 'Beauty and Personal Care'

    if 'entertainment' in industry_lower or any(x in company_lower for x in ['disney', 'directv']):
        return 'Entertainment and Streaming'

    if 'food' in industry_lower or 'beverage' in industry_lower or any(x in company_lower for x in ['coca-cola', 'bodyarmor']):
        return 'Food and Beverage'

    if 'retail' in industry_lower or any(x in company_lower for x in ['nike', 'ebay']):
        if any(x in company_lower for x in ['nike']):
            return 'Fashion and Accessories'
        return 'Retail and E-Commerce'

    if 'automotive' in industry_lower or any(x in company_lower for x in ['ford', 'jeep']):
        return 'Automotive'

    if 'software' in industry_lower or 'computer software' in industry_lower:
        return 'Software'

    if 'electronics' in industry_lower or 'consumer electronics' in industry_lower:
        return 'Electronics and Technology'

    if any(x in industry_lower for x in ['health', 'hospital']):
        return 'Health, Wellness, and Fitness'

    if 'apparel' in industry_lower or 'fashion' in industry_lower:
        return 'Fashion and Accessories'

    if 'electric' in industry_lower and 'manufacturing' in industry_lower:
        return 'Electronics and Technology'

    if 'telecommunication' in industry_lower:
        return 'Telecommunications'

    # Default to a reasonable category
    return 'Consumer Goods (FMCG/CPG)'

# Process each lead
enriched_leads = []
matches_found = 0

for lead in batch_data['leads']:
    company_name = lead.get('inline-flex', '')
    industry = lead.get('font-qanelas 14', lead.get('font-qanelas 13', ''))

    # Find brand match
    brand, brand_info = find_brand_match(company_name)

    # Create enriched lead with all original fields
    enriched_lead = lead.copy()

    if brand and brand_info:
        enriched_lead['brand_in_golden_sheet'] = 'Yes'
        enriched_lead['total_assets_tested'] = brand_info['grand_total']
        enriched_lead['platforms_tested'] = brand_info['platforms']
        enriched_lead['markets_tested'] = brand_info['markets']
        matches_found += 1
    else:
        enriched_lead['brand_in_golden_sheet'] = 'No'
        enriched_lead['total_assets_tested'] = ''
        enriched_lead['platforms_tested'] = ''
        enriched_lead['markets_tested'] = ''

    # Categorize
    category = categorize_company(company_name, industry)
    enriched_lead['company_category'] = category
    enriched_lead['category_asset_count'] = categories.get(category, 0)

    enriched_leads.append(enriched_lead)

# Create output directory if it doesn't exist
os.makedirs('/home/user/ClaudeCodeTest/enriched_results', exist_ok=True)

# Save to CSV
output_file = '/home/user/ClaudeCodeTest/enriched_results/enriched_batch_4.csv'
if enriched_leads:
    fieldnames = list(enriched_leads[0].keys())

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_leads)

# Print summary
print(f"Processed {len(enriched_leads)} leads")
print(f"Matched {matches_found} to Golden Sheet brands")
print(f"\nTop 3 companies with enrichment:")

# Show top 3 matched companies
shown = 0
for lead in enriched_leads:
    if lead['brand_in_golden_sheet'] == 'Yes' and shown < 3:
        print(f"\n{shown + 1}. {lead['inline-flex']}")
        print(f"   - Name: {lead['truncate']}")
        print(f"   - Total Assets: {lead['total_assets_tested']}")
        print(f"   - Platforms: {lead['platforms_tested']}")
        print(f"   - Markets: {lead['markets_tested']}")
        print(f"   - Category: {lead['company_category']} ({lead['category_asset_count']} assets)")
        shown += 1
