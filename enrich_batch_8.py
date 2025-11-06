import json
import csv
from difflib import SequenceMatcher
import re
import os

# Fuzzy matching function
def fuzzy_match(str1, str2, threshold=0.75):
    """Check if two strings are similar enough"""
    if not str1 or not str2:
        return False
    str1 = str1.lower().strip()
    str2 = str2.lower().strip()
    ratio = SequenceMatcher(None, str1, str2).ratio()
    return ratio >= threshold

def normalize_brand_name(name):
    """Normalize brand name for matching"""
    if not name:
        return ""
    name = name.lower().strip()
    # Remove common suffixes
    name = re.sub(r'\s+(inc\.|incorporated|corp\.|corporation|company|co\.|ltd\.|limited|the)\s*$', '', name, flags=re.IGNORECASE)
    # Remove special characters
    name = re.sub(r'[^\w\s-]', '', name)
    # Normalize whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def match_brand_to_pivot(company_name, pivot_data):
    """Match company name to pivot table brands with fuzzy matching"""
    if not company_name:
        return None

    normalized_company = normalize_brand_name(company_name)

    # Direct matches
    for row in pivot_data:
        brand = row.get('brand', '')
        if not brand or brand in ['Main Brand', 'Grand Total', '']:
            continue

        normalized_brand = normalize_brand_name(brand)

        # Exact match
        if normalized_company == normalized_brand:
            return row

        # Check if one contains the other
        if normalized_company in normalized_brand or normalized_brand in normalized_company:
            return row

        # Fuzzy match
        if fuzzy_match(normalized_company, normalized_brand, 0.85):
            return row

    # Special cases for variations
    company_lower = company_name.lower()

    # Amazon variations
    if 'amazon' in company_lower:
        for row in pivot_data:
            brand = row.get('brand', '')
            if 'amazon' in brand.lower():
                return row

    # Disney variations
    if 'disney' in company_lower or 'walt disney' in company_lower:
        for row in pivot_data:
            brand = row.get('brand', '')
            if 'disney' in brand.lower():
                return row

    # Estée Lauder variations
    if 'estee' in company_lower or 'estée' in company_lower or 'lauder' in company_lower:
        for row in pivot_data:
            brand = row.get('brand', '')
            if 'lauder' in brand.lower() or 'estée' in brand.lower() or 'estee' in brand.lower():
                return row

    # T-Mobile variations
    if 't-mobile' in company_lower or 'tmobile' in company_lower or 'metro by t-mobile' in company_lower:
        for row in pivot_data:
            brand = row.get('brand', '')
            if 't-mobile' in brand.lower():
                return row

    # Coca Cola variations
    if 'coca' in company_lower or 'coca-cola' in company_lower or 'coke' in company_lower:
        for row in pivot_data:
            brand = row.get('brand', '')
            if 'coca' in brand.lower():
                return row

    # Ford variations
    if 'ford motor' in company_lower or company_lower == 'ford':
        for row in pivot_data:
            brand = row.get('brand', '')
            if brand.lower() == 'ford':
                return row

    # Sainsbury's variations
    if 'sainsbury' in company_lower:
        for row in pivot_data:
            brand = row.get('brand', '')
            if 'sainsbury' in brand.lower():
                return row

    # Nike variations
    if 'nike' in company_lower:
        for row in pivot_data:
            brand = row.get('brand', '')
            if brand.lower() == 'nike':
                return row

    # Under Armour variations
    if 'under armour' in company_lower:
        for row in pivot_data:
            brand = row.get('brand', '')
            if 'under armour' in brand.lower():
                return row

    return None

def categorize_company(company_name, industry):
    """Categorize company based on name and industry"""
    if not company_name:
        return "Services (Professional and Consumer)", 44

    company_lower = company_name.lower()
    industry_lower = str(industry).lower() if industry else ""

    # Automotive
    if 'automotive' in industry_lower or any(x in company_lower for x in ['ford', 'lincoln']):
        return "Automotive", 64

    # Beauty and Personal Care
    if any(x in industry_lower for x in ['cosmetics', 'beauty', 'personal care']) or \
       any(x in company_lower for x in ['estee', 'estée', 'lauder', 'la mer', 'clinique']):
        return "Beauty and Personal Care", 400

    # Electronics and Technology
    if any(x in industry_lower for x in ['computer software', 'software', 'electronics', 'technology']) or \
       any(x in company_lower for x in ['amazon', 'shopify']):
        return "Electronics and Technology", 254

    # Entertainment and Streaming
    if 'entertainment' in industry_lower or 'disney' in company_lower:
        return "Entertainment and Streaming", 146

    # Retail and E-Commerce
    if any(x in industry_lower for x in ['retail', 'e-commerce']) or \
       any(x in company_lower for x in ['sainsbury', 'walmart', 'target']):
        return "Retail and E-Commerce", 212

    # Fashion and Accessories
    if any(x in industry_lower for x in ['apparel', 'fashion', 'accessories']) or \
       any(x in company_lower for x in ['nike', 'under armour', 'hugo boss']):
        return "Fashion and Accessories", 227

    # Telecommunications
    if 'telecommunication' in industry_lower or 't-mobile' in company_lower:
        return "Telecommunications", 113

    # Food and Beverage
    if any(x in industry_lower for x in ['food', 'beverage']) or \
       any(x in company_lower for x in ['coca-cola', 'coca cola', 'coke']):
        return "Food and Beverage", 239

    # Marketing & Advertising
    if 'marketing' in industry_lower or 'advertising' in industry_lower:
        return "Services (Professional and Consumer)", 44

    # Market Research
    if 'market research' in industry_lower:
        return "Services (Professional and Consumer)", 44

    # Non-profit
    if 'non-profit' in industry_lower:
        return "Charities, Foundations & NGOs", 16

    # Education
    if 'education' in industry_lower:
        return "Education and Training", 9

    # Default
    return "Services (Professional and Consumer)", 44

# Read input files
print("Reading input files...")
with open('/home/user/ClaudeCodeTest/agent_batches/enrichment_batch_8.json', 'r') as f:
    batch_data = json.load(f)

# Read categories CSV
categories_data = {}
with open('/home/user/ClaudeCodeTest/Golden Sheet - Category_Count.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        categories_data[row['Primary Category']] = int(row['Number of Assets Tested'])

# Read pivot table CSV - handle the complex structure
pivot_data = []
with open('/home/user/ClaudeCodeTest/Golden Sheet - Pivot Table Brands.csv', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    # Parse manually due to complex CSV structure
    # Line 0: headers (row number, Main Brand, platforms...)
    # Line 1: subheaders
    # Line 2: empty row
    # Line 3+: data

    for i, line in enumerate(lines):
        if i < 3:  # Skip header rows
            continue

        parts = line.strip().split(',')
        if len(parts) < 3:
            continue

        # Structure: row_num, brand, amazon_prime, instagram, netflix, standalone, tiktok, youtube_shorts, grand_total, platform_list, markets
        row_num = parts[0]
        brand = parts[1].strip()

        if not brand or brand in ['Main Brand', 'Grand Total', 'MAIN BRAND']:
            continue

        # Extract data
        try:
            grand_total_idx = 8
            grand_total = parts[grand_total_idx] if len(parts) > grand_total_idx else ''

            # Platform columns (indices 2-7)
            platforms = []
            platform_names = ['amazon_prime', 'instagram', 'netflix', 'standalone', 'tiktok', 'youtube_shorts']
            for idx, pname in enumerate(platform_names, start=2):
                if len(parts) > idx and parts[idx] and parts[idx].strip() and parts[idx].strip() != '':
                    try:
                        if int(parts[idx]) > 0:
                            platforms.append(pname)
                    except:
                        pass

            # Markets are in the last part
            markets = ''
            if len(parts) > 10:
                # Markets might be quoted and contain commas
                markets_part = ','.join(parts[10:]).strip().strip('"')
                markets = markets_part

            pivot_data.append({
                'brand': brand,
                'grand_total': grand_total,
                'platforms': platforms,
                'markets': markets
            })
        except Exception as e:
            print(f"Error parsing row {i}: {e}")
            continue

print(f"Loaded {len(pivot_data)} brands from pivot table")
if pivot_data:
    print(f"Sample brands: {', '.join([p['brand'] for p in pivot_data[:5]])}")

leads = batch_data['leads']
enriched_leads = []
matched_count = 0
top_companies = []

print(f"\nProcessing {len(leads)} leads...")

for i, lead in enumerate(leads, 1):
    # Extract company name
    company_name = lead.get('inline-flex', '').strip()

    # Get industry from various fields
    industry = lead.get('font-qanelas 13', '') or lead.get('font-qanelas 14', '') or \
               lead.get('font-qanelas 17', '') or lead.get('font-qanelas 12', '')

    # Try to match to pivot table
    matched_row = match_brand_to_pivot(company_name, pivot_data)

    # Add enrichment data
    enriched_lead = lead.copy()

    if matched_row is not None:
        matched_count += 1
        enriched_lead['brand_in_golden_sheet'] = 'Yes'

        # Get Grand Total
        grand_total = matched_row.get('grand_total', '')
        try:
            total_assets = int(grand_total) if grand_total else 0
        except:
            total_assets = 0

        enriched_lead['total_assets_tested'] = total_assets

        # Get platforms
        platforms = matched_row.get('platforms', [])
        enriched_lead['platforms_tested'] = ', '.join(platforms) if platforms else ''

        # Get markets
        markets = matched_row.get('markets', '')
        enriched_lead['markets_tested'] = markets

        # Save top companies for summary
        if len(top_companies) < 3:
            top_companies.append({
                'company': company_name,
                'brand': matched_row.get('brand', ''),
                'total_assets': total_assets,
                'platforms': enriched_lead['platforms_tested'],
                'markets': markets
            })

    else:
        enriched_lead['brand_in_golden_sheet'] = 'No'
        enriched_lead['total_assets_tested'] = ''
        enriched_lead['platforms_tested'] = ''
        enriched_lead['markets_tested'] = ''

    # Categorize company
    category, asset_count = categorize_company(company_name, industry)
    enriched_lead['company_category'] = category
    enriched_lead['category_asset_count'] = asset_count

    enriched_leads.append(enriched_lead)

    if i <= 5:  # Print first 5 for verification
        print(f"\n{i}. {company_name}")
        print(f"   Industry: {industry}")
        print(f"   Matched: {enriched_lead['brand_in_golden_sheet']}")
        if enriched_lead['brand_in_golden_sheet'] == 'Yes':
            print(f"   Brand: {matched_row.get('brand', '')}")
        print(f"   Total Assets: {enriched_lead['total_assets_tested']}")
        print(f"   Category: {category} ({asset_count} assets)")

# Create output directory if needed
os.makedirs('/home/user/ClaudeCodeTest/enriched_results', exist_ok=True)

# Write to CSV
output_file = '/home/user/ClaudeCodeTest/enriched_results/enriched_batch_8.csv'
if enriched_leads:
    keys = enriched_leads[0].keys()
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(enriched_leads)

print(f"\n{'='*60}")
print(f"ENRICHMENT SUMMARY - BATCH 8")
print(f"{'='*60}")
print(f"Total leads processed: {len(leads)}")
print(f"Leads matched to Golden Sheet: {matched_count}")
print(f"Match rate: {matched_count/len(leads)*100:.1f}%")
print(f"\nTop 3 Companies Found:")
for i, comp in enumerate(top_companies, 1):
    print(f"{i}. {comp['company']} (matched to: {comp['brand']})")
    print(f"   - Total Assets Tested: {comp['total_assets']}")
    print(f"   - Platforms: {comp['platforms']}")
    print(f"   - Markets: {comp['markets']}")
print(f"\nOutput saved to: {output_file}")
print(f"{'='*60}")
