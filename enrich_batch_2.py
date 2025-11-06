import json
import csv
import re
from difflib import SequenceMatcher

# Read batch 2 JSON
with open('/home/user/ClaudeCodeTest/agent_batches/enrichment_batch_2.json', 'r') as f:
    batch_data = json.load(f)

# Read category count CSV
categories = {}
with open('/home/user/ClaudeCodeTest/Golden Sheet - Category_Count.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        categories[row['Primary Category']] = int(row['Number of Assets Tested'])

# Read pivot table CSV
brands_data = {}
with open('/home/user/ClaudeCodeTest/Golden Sheet - Pivot Table Brands.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    rows = list(reader)
    # Skip first 3 rows (headers and summary)
    for row in rows[3:]:
        if len(row) >= 11:
            brand_name = row[1].strip()
            if brand_name and brand_name != 'Main Brand' and brand_name:
                # Extract Grand Total (column 8), Platforms (column 9), Markets (column 10)
                grand_total = row[8].strip() if len(row) > 8 else ''
                platforms = row[9].strip() if len(row) > 9 else ''
                markets = row[10].strip() if len(row) > 10 else ''

                brands_data[brand_name] = {
                    'total_assets': grand_total,
                    'platforms': platforms,
                    'markets': markets
                }

# Fuzzy matching function
def fuzzy_match(company, brand, threshold=0.7):
    """Returns similarity score between company and brand names"""
    company_lower = company.lower().strip()
    brand_lower = brand.lower().strip()

    # Direct match
    if company_lower == brand_lower:
        return 1.0

    # Check if one contains the other
    if company_lower in brand_lower or brand_lower in company_lower:
        return 0.9

    # Handle common variations
    variations = {
        'the walt disney company': ['disney'],
        'amazon': ['amazon', 'amazon business', 'amazon music', 'prime video & amazon mgm studios'],
        'apple': ['apple'],
        'sharkninja': ['ninja', 'sharkninja'],
        'the coca-cola company': ['coca cola', 'coke', 'coca-cola'],
        't-mobile': ['tmobile', 'metro by t-mobile'],
        'nike': ['nike'],
        'under armour': ['under armour'],
        'wayfair': ['wayfair'],
        'ford motor company': ['ford'],
        'petsmart': ['petsmart'],
        'pets at home': ['pets at home'],
        'ebay': ['ebay'],
        'amazon prime': ['prime video & amazon mgm studios', 'amazon']
    }

    for key, values in variations.items():
        if company_lower in key or key in company_lower:
            for val in values:
                if val.lower() in brand_lower or brand_lower in val.lower():
                    return 0.95

    # Sequence matcher
    return SequenceMatcher(None, company_lower, brand_lower).ratio()

# Match company to brand
def match_brand(company_name):
    """Find best matching brand in pivot table"""
    best_match = None
    best_score = 0.6  # Minimum threshold

    for brand in brands_data.keys():
        score = fuzzy_match(company_name, brand)
        if score > best_score:
            best_score = score
            best_match = brand

    return best_match

# Categorize company
def categorize_company(company_name, industry):
    """Assign a category to the company"""
    company_lower = company_name.lower()
    industry_lower = industry.lower() if industry else ''

    # Category mapping based on company name and industry
    if any(x in company_lower for x in ['disney', 'music', 'entertainment', 'video', 'netflix', 'prime video']) or 'entertainment' in industry_lower:
        return 'Entertainment and Streaming'
    elif any(x in company_lower for x in ['amazon', 'ebay', 'wayfair', 'viator']) or 'retail' in industry_lower or 'e-commerce' in industry_lower:
        return 'Retail and E-Commerce'
    elif any(x in company_lower for x in ['apple', 'google', 'microsoft']) or 'computer software' in industry_lower or 'consumer electronics' in industry_lower:
        return 'Electronics and Technology'
    elif any(x in company_lower for x in ['coca-cola', 'coke', 'pepsi']) or 'food' in industry_lower or 'beverage' in industry_lower:
        return 'Food and Beverage'
    elif any(x in company_lower for x in ['t-mobile', 'verizon', 'at&t']) or 'telecommunications' in industry_lower or 'telecom' in industry_lower:
        return 'Telecommunications'
    elif any(x in company_lower for x in ['nike', 'adidas', 'under armour', 'reebok']) or 'apparel' in industry_lower or 'fashion' in industry_lower:
        return 'Fashion and Accessories'
    elif any(x in company_lower for x in ['ford', 'toyota', 'honda']) or 'automotive' in industry_lower:
        return 'Automotive'
    elif any(x in company_lower for x in ['petsmart', 'pets at home', 'petco']) or 'pet' in industry_lower:
        return 'Pet Food & Care'
    elif any(x in company_lower for x in ['shark', 'ninja', 'philips']) or 'electrical' in industry_lower or 'electronic manufacturing' in industry_lower:
        return 'Electronics and Technology'
    elif 'travel' in industry_lower or 'tourism' in industry_lower or 'leisure' in industry_lower:
        return 'Travel, Tourism and Hospitality'
    elif 'publishing' in industry_lower:
        return 'Services (Professional and Consumer)'
    else:
        # Default to a reasonable category
        if 'software' in industry_lower:
            return 'Software'
        elif 'retail' in industry_lower:
            return 'Retail and E-Commerce'
        else:
            return 'Consumer Goods (FMCG/CPG)'

# Process each lead
enriched_leads = []
for lead in batch_data['leads']:
    company_name = lead.get('inline-flex', '')
    industry = lead.get('font-qanelas 13', '') or lead.get('font-qanelas 14', '')

    # Match to brand in pivot table
    matched_brand = match_brand(company_name) if company_name else None

    if matched_brand:
        brand_info = brands_data[matched_brand]
        lead['brand_in_golden_sheet'] = 'Yes'
        lead['total_assets_tested'] = brand_info['total_assets']
        lead['platforms_tested'] = brand_info['platforms']
        lead['markets_tested'] = brand_info['markets']
    else:
        lead['brand_in_golden_sheet'] = 'No'
        lead['total_assets_tested'] = ''
        lead['platforms_tested'] = ''
        lead['markets_tested'] = ''

    # Categorize company
    category = categorize_company(company_name, industry)
    lead['company_category'] = category
    lead['category_asset_count'] = categories.get(category, '')

    enriched_leads.append(lead)

# Write to CSV
import os
os.makedirs('/home/user/ClaudeCodeTest/enriched_results', exist_ok=True)

# Get all unique field names
all_fields = set()
for lead in enriched_leads:
    all_fields.update(lead.keys())

# Define field order - original fields first, then enrichment fields
enrichment_fields = ['brand_in_golden_sheet', 'total_assets_tested', 'platforms_tested', 'markets_tested', 'company_category', 'category_asset_count']
original_fields = [f for f in all_fields if f not in enrichment_fields]
fieldnames = original_fields + enrichment_fields

with open('/home/user/ClaudeCodeTest/enriched_results/enriched_batch_2.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(enriched_leads)

# Generate summary statistics
total_leads = len(enriched_leads)
matched_brands = sum(1 for lead in enriched_leads if lead['brand_in_golden_sheet'] == 'Yes')

# Get top 3 companies with enrichment data
top_companies = []
for lead in enriched_leads:
    if lead['brand_in_golden_sheet'] == 'Yes':
        top_companies.append({
            'company': lead.get('inline-flex', ''),
            'total_assets': lead.get('total_assets_tested', ''),
            'platforms': lead.get('platforms_tested', ''),
            'markets': lead.get('markets_tested', ''),
            'category': lead.get('company_category', '')
        })

# Sort by total assets (convert to int, handle empty strings)
def get_assets(company):
    try:
        return int(company['total_assets']) if company['total_assets'] else 0
    except:
        return 0

top_companies.sort(key=get_assets, reverse=True)

print("\n" + "="*80)
print("BATCH 2 ENRICHMENT SUMMARY")
print("="*80)
print(f"\nTotal leads processed: {total_leads}")
print(f"Leads matched to Golden Sheet brands: {matched_brands}")
print(f"Match rate: {matched_brands/total_leads*100:.1f}%")

print(f"\nTop 3 companies with enrichment data:")
print("-"*80)
for i, company in enumerate(top_companies[:3], 1):
    print(f"\n{i}. {company['company']}")
    print(f"   Total Assets Tested: {company['total_assets']}")
    print(f"   Platforms: {company['platforms']}")
    print(f"   Markets: {company['markets']}")
    print(f"   Category: {company['category']}")

print("\n" + "="*80)
print(f"Output saved to: enriched_results/enriched_batch_2.csv")
print("="*80 + "\n")
