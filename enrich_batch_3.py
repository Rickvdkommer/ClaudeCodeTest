import json
import csv
import re
from difflib import SequenceMatcher

# Read batch 3 JSON
with open('/home/user/ClaudeCodeTest/agent_batches/enrichment_batch_3.json', 'r') as f:
    batch_data = json.load(f)

# Read category count CSV
categories = {}
with open('/home/user/ClaudeCodeTest/Golden Sheet - Category_Count.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        categories[row['Primary Category']] = int(row['Number of Assets Tested'])

# Read pivot table CSV properly with csv module
brands = {}
with open('/home/user/ClaudeCodeTest/Golden Sheet - Pivot Table Brands.csv', 'r') as f:
    reader = csv.reader(f)
    # Skip first 3 header rows
    next(reader)
    next(reader)
    next(reader)

    for row in reader:
        if len(row) >= 11:
            brand_name = row[1].strip() if len(row) > 1 else ''

            if brand_name and brand_name != 'Grand Total' and brand_name != 'MAIN BRAND':
                try:
                    grand_total = int(row[8]) if len(row) > 8 and row[8].strip() else 0
                except:
                    grand_total = 0

                platforms = row[9].strip() if len(row) > 9 else ''
                markets = row[10].strip() if len(row) > 10 else ''

                brands[brand_name] = {
                    'total_assets': grand_total,
                    'platforms': platforms,
                    'markets': markets
                }

print(f"Loaded {len(brands)} brands from pivot table")

# Function to normalize company names for matching
def normalize_name(name):
    if not name:
        return ""
    # Remove common suffixes and normalize
    name = name.lower().strip()
    name = re.sub(r'\s*(inc\.|incorporated|company|corp\.|corporation|ltd\.|limited|llc|the)\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'^the\s+', '', name)
    name = re.sub(r'[^\w\s]', '', name)  # Remove punctuation
    name = re.sub(r'\s+', ' ', name)  # Normalize spaces
    return name.strip()

# Function to find best brand match
def find_brand_match(company_name):
    if not company_name:
        return None, 0

    normalized_company = normalize_name(company_name)

    # Special case mappings for common variations - ORDER MATTERS (most specific first)
    mappings = {
        'amazon web services aws': 'AWS',
        'amazon web services': 'AWS',
        'aws': 'AWS',
        'prime video  amazon mgm studios': 'Amazon',
        'prime video amazon mgm studios': 'Amazon',
        'prime video': 'Amazon',
        'amazon mgm studios': 'Amazon',
        't mobile': 'T-Mobile',
        'tmobile': 'T-Mobile',
        'metro by t mobile': 'Metro by T-Mobile',
        'sharkninja': 'NINJA',
        'coca cola company': 'Coca Cola',
        'coca-cola company': 'Coca Cola',
        'cocacola': 'Coca Cola',
        'walt disney company': 'Disney',
        'estee lauder companies': 'Estée Lauder',
        'estée lauder companies': 'Estée Lauder',
        'ford motor company': 'Ford',
        'samsung electronics': 'Samsung',
        'samsung galaxy': 'Samsung Galaxy',
    }

    if normalized_company in mappings:
        mapped_brand = mappings[normalized_company]
        if mapped_brand in brands:
            return mapped_brand, 1.0

    # Exact match first
    for brand in brands.keys():
        if normalize_name(brand) == normalized_company:
            return brand, 1.0

    # Check for partial matches where company name contains brand or vice versa
    for brand in brands.keys():
        normalized_brand = normalize_name(brand)

        # Special handling for common cases
        if normalized_brand and normalized_company:
            # If brand is in company name or company is in brand name
            if len(normalized_brand) >= 4:  # Avoid matching very short names
                if normalized_brand in normalized_company or normalized_company in normalized_brand:
                    # Make sure it's a good match
                    if len(normalized_brand) > 3 and len(normalized_company) > 3:
                        return brand, 0.95

    # Fuzzy match for remaining cases
    best_match = None
    best_score = 0

    for brand in brands.keys():
        normalized_brand = normalize_name(brand)
        score = SequenceMatcher(None, normalized_company, normalized_brand).ratio()

        if score > best_score and score > 0.75:
            best_score = score
            best_match = brand

    return best_match, best_score

# Function to categorize company
def categorize_company(company_name, industry):
    company_lower = company_name.lower() if company_name else ""
    industry_lower = industry.lower() if industry else ""

    # Define category mappings - ORDER MATTERS (most specific first)

    # Check for cloud/software services FIRST before checking for Amazon retail
    if any(x in company_lower for x in ['aws', 'web services', 'azure', 'google cloud']):
        return 'Software'
    elif 'information technology' in industry_lower and 'services' in industry_lower:
        return 'Software'

    elif any(x in company_lower for x in ['apple', 'samsung', 'google', 'microsoft', 'dell', 'hp', 'sony', 'lg', 'philips', 'dyson', 'ninja', 'sharkninja']):
        return 'Electronics and Technology'
    elif any(x in industry_lower for x in ['consumer electronics', 'electrical', 'electronic manufacturing']):
        return 'Electronics and Technology'

    elif any(x in company_lower for x in ['amazon', 'ebay', 'walmart', 'target', 'petsmart', 'argos']):
        return 'Retail and E-Commerce'
    elif 'retail' in industry_lower:
        return 'Retail and E-Commerce'

    elif any(x in company_lower for x in ['disney', 'netflix', 'hbo', 'prime video', 'spotify', 'apple tv', 'mgm']):
        return 'Entertainment and Streaming'
    elif 'entertainment' in industry_lower:
        return 'Entertainment and Streaming'

    elif any(x in company_lower for x in ['nike', 'adidas', 'levi', 'h&m', 'zara', 'gap', 'under armour', 'reebok']):
        return 'Fashion and Accessories'

    elif any(x in company_lower for x in ['coca cola', 'pepsi', 'starbucks', 'mcdonalds', 'burger king', 'kfc']):
        return 'Food and Beverage'
    elif 'food' in industry_lower or 'beverage' in industry_lower or 'restaurant' in industry_lower:
        return 'Food and Beverage'

    elif any(x in company_lower for x in ['estee lauder', 'loreal', 'clinique', 'mac', 'sephora', 'ulta']):
        return 'Beauty and Personal Care'
    elif 'cosmetics' in industry_lower or 'beauty' in industry_lower:
        return 'Beauty and Personal Care'

    elif any(x in company_lower for x in ['t-mobile', 'verizon', 'att', 'vodafone', 'sprint']):
        return 'Telecommunications'
    elif 'telecommunications' in industry_lower or 'telecom' in industry_lower:
        return 'Telecommunications'

    elif any(x in company_lower for x in ['ford', 'gm', 'tesla', 'toyota', 'honda', 'bmw', 'jeep', 'waymo']):
        return 'Automotive'
    elif 'automotive' in industry_lower or 'motor' in industry_lower:
        return 'Automotive'

    elif any(x in company_lower for x in ['microsoft', 'oracle', 'salesforce', 'adobe', 'sap', 'aws', 'web services']):
        return 'Software'
    elif 'software' in industry_lower or 'computer software' in industry_lower:
        return 'Software'
    elif 'information technology' in industry_lower:
        return 'Software'

    elif any(x in company_lower for x in ['petsmart', 'petco', 'chewy']):
        return 'Pet Food & Care'

    elif 'aviation' in industry_lower or 'aerospace' in industry_lower:
        return 'Electronics and Technology'

    # Default to Services or Technology
    if 'services' in industry_lower:
        return 'Services (Professional and Consumer)'
    else:
        return 'Electronics and Technology'

# Process each lead
enriched_leads = []

for lead in batch_data['leads']:
    company = lead.get('inline-flex', '')
    industry = lead.get('font-qanelas 14', lead.get('font-qanelas 18', lead.get('font-qanelas 13', '')))

    # Find brand match
    brand_match, match_score = find_brand_match(company)

    # Create enriched record
    enriched = lead.copy()

    if brand_match and match_score >= 0.75:
        brand_data = brands[brand_match]
        enriched['brand_in_golden_sheet'] = 'Yes'
        enriched['total_assets_tested'] = brand_data['total_assets']
        enriched['platforms_tested'] = brand_data['platforms']
        enriched['markets_tested'] = brand_data['markets']
    else:
        enriched['brand_in_golden_sheet'] = 'No'
        enriched['total_assets_tested'] = ''
        enriched['platforms_tested'] = ''
        enriched['markets_tested'] = ''

    # Categorize
    category = categorize_company(company, industry)
    enriched['company_category'] = category
    enriched['category_asset_count'] = categories.get(category, 0)

    enriched_leads.append(enriched)

# Write to CSV
import os
os.makedirs('/home/user/ClaudeCodeTest/enriched_results', exist_ok=True)

output_file = '/home/user/ClaudeCodeTest/enriched_results/enriched_batch_3.csv'

if enriched_leads:
    # Get all unique field names
    fieldnames = list(enriched_leads[0].keys())

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_leads)

# Generate summary
total_leads = len(enriched_leads)
matched_leads = sum(1 for lead in enriched_leads if lead['brand_in_golden_sheet'] == 'Yes')

# Get top companies with their enrichment data
company_data = {}
for lead in enriched_leads:
    company = lead.get('inline-flex', 'Unknown')
    if company not in company_data:
        company_data[company] = {
            'count': 0,
            'brand_in_gs': lead['brand_in_golden_sheet'],
            'total_assets': lead['total_assets_tested'],
            'platforms': lead['platforms_tested'],
            'markets': lead['markets_tested'],
            'category': lead['company_category'],
            'category_asset_count': lead['category_asset_count']
        }
    company_data[company]['count'] += 1

# Sort by count
top_companies = sorted(company_data.items(), key=lambda x: x[1]['count'], reverse=True)[:3]

print(f"\nBATCH 3 ENRICHMENT COMPLETE")
print(f"=" * 70)
print(f"Total leads processed: {total_leads}")
print(f"Matched to Golden Sheet: {matched_leads} ({matched_leads/total_leads*100:.1f}%)")
print(f"\nTop 3 Companies Found:\n")

for i, (company, data) in enumerate(top_companies, 1):
    print(f"{i}. {company}")
    print(f"   Lead count: {data['count']}")
    print(f"   In Golden Sheet: {data['brand_in_gs']}")
    if data['brand_in_gs'] == 'Yes':
        print(f"   Total assets tested: {data['total_assets']}")
        print(f"   Platforms: {data['platforms']}")
        print(f"   Markets: {data['markets']}")
    print(f"   Category: {data['category']}")
    print(f"   Category asset count: {data['category_asset_count']}")
    print()

print(f"Output saved to: {output_file}")
