import json
import csv
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher

def similarity(a: str, b: str) -> float:
    """Calculate similarity ratio between two strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def fuzzy_match_brand(company_name: str, brands: List[str], threshold: float = 0.7) -> Optional[str]:
    """Find the best matching brand using fuzzy matching."""
    best_match = None
    best_score = 0

    company_lower = company_name.lower()

    # First try exact match
    for brand in brands:
        if company_lower == brand.lower():
            return brand

    # Handle common variations
    variations = {
        'the coca-cola company': 'Coca Cola',
        'coca-cola': 'Coca Cola',
        'the walt disney company': 'Disney',
        'walt disney': 'Disney',
        't-mobile': 'T-Mobile',
        'tmobile': 'T-Mobile',
        'the estée lauder companies inc.': 'Estée Lauder',
        'estee lauder': 'Estée Lauder',
        'sharkninja': 'NINJA',
        'sainsbury\'s': 'Sainsbury\'s',
        'tim hortons': 'Tim Hortons',
        'samsung electronics': 'Samsung',
        'burger king': 'Burger King',
        'mozilla': 'Mozilla Firefox',
        'ford motor company': 'Ford'
    }

    if company_lower in variations:
        target = variations[company_lower]
        for brand in brands:
            if brand.lower() == target.lower():
                return brand

    # Try fuzzy matching
    for brand in brands:
        score = similarity(company_name, brand)
        if score > best_score:
            best_score = score
            best_match = brand

    return best_match if best_score >= threshold else None

def categorize_company(company_name: str, industry: str) -> str:
    """Categorize a company based on name and industry."""
    company_lower = company_name.lower()
    industry_lower = industry.lower() if industry else ""

    # Electronics & Tech
    if any(x in company_lower for x in ['apple', 'amazon', 'samsung', 'mozilla', 'ebay']):
        return 'Electronics and Technology'
    if any(x in industry_lower for x in ['electronics', 'computer software', 'software']):
        return 'Electronics and Technology'

    # Food & Beverage
    if any(x in company_lower for x in ['coca-cola', 'burger king', 'tim hortons']):
        return 'Food and Beverage'
    if 'food' in industry_lower or 'beverage' in industry_lower or 'restaurant' in industry_lower:
        return 'Food and Beverage'

    # Beauty & Personal Care
    if any(x in company_lower for x in ['estée lauder', 'estee lauder']):
        return 'Beauty and Personal Care'
    if 'cosmetic' in industry_lower or 'beauty' in industry_lower:
        return 'Beauty and Personal Care'

    # Automotive
    if 'ford' in company_lower:
        return 'Automotive'
    if 'automotive' in industry_lower:
        return 'Automotive'

    # Entertainment
    if 'disney' in company_lower:
        return 'Entertainment and Streaming'
    if 'entertainment' in industry_lower:
        return 'Entertainment and Streaming'

    # Retail
    if any(x in company_lower for x in ['nike', 'sainsbury']):
        return 'Retail and E-Commerce'
    if 'retail' in industry_lower:
        return 'Retail and E-Commerce'

    # Telecommunications
    if 't-mobile' in company_lower:
        return 'Telecommunications'
    if 'telecommunication' in industry_lower:
        return 'Telecommunications'

    # Default categorization based on industry
    if 'manufacturing' in industry_lower:
        return 'Consumer Goods (FMCG/CPG)'

    return 'Services (Professional and Consumer)'

# Load batch data
with open('/home/user/ClaudeCodeTest/agent_batches/enrichment_batch_6.json', 'r') as f:
    batch_data = json.load(f)

# Load category counts
categories = {}
with open('/home/user/ClaudeCodeTest/Golden Sheet - Category_Count.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        categories[row['Primary Category']] = row['Number of Assets Tested']

# Load pivot table brands
brands_data = {}
with open('/home/user/ClaudeCodeTest/Golden Sheet - Pivot Table Brands.csv', 'r') as f:
    reader = csv.reader(f)
    headers = next(reader)
    next(reader)  # Skip second header row

    for row in reader:
        if len(row) < 11 or not row[1]:  # Skip empty rows
            continue

        brand_name = row[1]
        if brand_name in ['MAIN BRAND', 'Grand Total']:
            continue

        grand_total = row[8] if len(row) > 8 else '0'
        platforms = row[9] if len(row) > 9 else ''
        markets = row[10] if len(row) > 10 else ''

        brands_data[brand_name] = {
            'total_assets': grand_total,
            'platforms': platforms,
            'markets': markets
        }

brand_names = list(brands_data.keys())

# Process each lead
enriched_leads = []
matches_found = 0

for lead in batch_data['leads']:
    company_name = lead.get('inline-flex', '')
    industry = lead.get('font-qanelas 14', '')

    # Match to brand
    matched_brand = fuzzy_match_brand(company_name, brand_names)

    # Create enriched record with all original fields
    enriched = lead.copy()

    if matched_brand and matched_brand in brands_data:
        brand_info = brands_data[matched_brand]
        enriched['brand_in_golden_sheet'] = 'Yes'
        enriched['total_assets_tested'] = brand_info['total_assets']
        enriched['platforms_tested'] = brand_info['platforms']
        enriched['markets_tested'] = brand_info['markets']
        matches_found += 1
    else:
        enriched['brand_in_golden_sheet'] = 'No'
        enriched['total_assets_tested'] = ''
        enriched['platforms_tested'] = ''
        enriched['markets_tested'] = ''

    # Categorize company
    category = categorize_company(company_name, industry)
    enriched['company_category'] = category
    enriched['category_asset_count'] = categories.get(category, '0')

    enriched_leads.append(enriched)

# Write to CSV
output_path = '/home/user/ClaudeCodeTest/enriched_results/enriched_batch_6.csv'

# Get all possible field names
all_fields = set()
for lead in enriched_leads:
    all_fields.update(lead.keys())

# Define field order - original fields first, then enrichment fields
original_fields = list(batch_data['leads'][0].keys())
enrichment_fields = ['brand_in_golden_sheet', 'total_assets_tested', 'platforms_tested',
                     'markets_tested', 'company_category', 'category_asset_count']
fieldnames = original_fields + enrichment_fields

with open(output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(enriched_leads)

# Print summary
print(f"Batch 6 Enrichment Complete!")
print(f"=" * 60)
print(f"Total leads processed: {len(enriched_leads)}")
print(f"Matched to Golden Sheet: {matches_found}")
print(f"Match rate: {matches_found/len(enriched_leads)*100:.1f}%")
print()

# Show top 3 companies with enrichment
print("Top 3 Companies with Enrichment Data:")
print("=" * 60)

top_companies = []
for lead in enriched_leads:
    if lead['brand_in_golden_sheet'] == 'Yes':
        top_companies.append(lead)
    if len(top_companies) == 3:
        break

for i, lead in enumerate(top_companies, 1):
    print(f"\n{i}. {lead.get('inline-flex', 'Unknown Company')}")
    print(f"   Contact: {lead.get('truncate', 'N/A')}")
    print(f"   Total Assets Tested: {lead.get('total_assets_tested', 'N/A')}")
    print(f"   Platforms: {lead.get('platforms_tested', 'N/A')}")
    print(f"   Markets: {lead.get('markets_tested', 'N/A')}")
    print(f"   Category: {lead.get('company_category', 'N/A')} ({lead.get('category_asset_count', 'N/A')} assets)")
