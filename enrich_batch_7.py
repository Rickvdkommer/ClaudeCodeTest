#!/usr/bin/env python3
"""
Enrichment script for batch 7 leads
Matches companies to Golden Sheet brands and categorizes them
"""

import json
import csv
import re
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher

def normalize_name(name: str) -> str:
    """Normalize company name for matching"""
    if not name:
        return ""
    # Convert to lowercase, remove special chars, extra spaces
    name = name.lower()
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def fuzzy_match(name1: str, name2: str) -> float:
    """Calculate similarity score between two names"""
    return SequenceMatcher(None, normalize_name(name1), normalize_name(name2)).ratio()

def load_pivot_table(filepath: str) -> List[Dict]:
    """Load pivot table with brand data"""
    brands = []
    with open(filepath, 'r', encoding='utf-8') as f:
        # Read all lines
        lines = list(csv.reader(f))

        # Skip first two header rows and the summary row, start from row 3 (index 3)
        for i, row in enumerate(lines):
            if i < 3:  # Skip headers and summary
                continue

            if len(row) < 2:
                continue

            # Column structure: [index, Main Brand, amazon_prime, instagram, netflix, standalone, tiktok, youtube_shorts, Grand Total, platforms, markets]
            brand_name = row[1].strip() if len(row) > 1 else ''

            if not brand_name or brand_name == 'Grand Total' or brand_name == 'Main Brand':
                continue

            # Get grand total (column 8)
            grand_total = row[8].strip() if len(row) > 8 else ''

            # Get platforms list (column 9) and markets list (column 10)
            platforms_str = row[9].strip() if len(row) > 9 else ''
            markets_str = row[10].strip() if len(row) > 10 else ''

            # Parse individual platform columns to build platforms list
            platforms = []
            platform_cols = {
                2: 'amazon_prime',
                3: 'instagram',
                4: 'netflix',
                5: 'standalone',
                6: 'tiktok',
                7: 'youtube_shorts'
            }

            for col_idx, platform_name in platform_cols.items():
                if len(row) > col_idx and row[col_idx].strip():
                    try:
                        if int(row[col_idx]) > 0:
                            platforms.append(platform_name)
                    except:
                        pass

            brands.append({
                'brand': brand_name,
                'total_assets': grand_total,
                'platforms': platforms,
                'platforms_str': platforms_str if platforms_str else ', '.join(platforms),
                'markets': markets_str,
            })

    return brands

def load_categories(filepath: str) -> Dict[str, int]:
    """Load category counts"""
    categories = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cat = row.get('Primary Category', '').strip()
            count = row.get('Number of Assets Tested', '0').strip()
            if cat:
                try:
                    categories[cat] = int(count)
                except:
                    categories[cat] = 0
    return categories

def match_brand(company_name: str, brands: List[Dict], threshold: float = 0.75) -> Optional[Dict]:
    """Match company to brand using fuzzy matching"""
    if not company_name:
        return None

    best_match = None
    best_score = threshold

    # Direct matches and variations
    variations = {
        'amazon': ['amazon', 'amazon fashion', 'amazon luxury stores', 'amazon fresh', 'amazon astro'],
        'coca-cola': ['coca cola', 'coca-cola', 'coke'],
        't-mobile': ['t-mobile', 'tmobile', 'metro by t-mobile'],
        'disney': ['disney', 'walt disney', 'disney+'],
        'estee lauder': ['estee lauder', 'estée lauder', 'elc'],
        'mac': ['mac cosmetics', 'mac', 'm.a.c'],
        'nike': ['nike'],
        'ford': ['ford motor company', 'ford'],
        'apple': ['apple'],
    }

    company_norm = normalize_name(company_name)

    # Check exact and variation matches first
    for brand_data in brands:
        brand_norm = normalize_name(brand_data['brand'])

        # Check if company matches brand directly
        if company_norm == brand_norm:
            return brand_data

        # Check variations
        for key, vals in variations.items():
            if any(v in company_norm for v in vals) and key in brand_norm:
                return brand_data

    # Fuzzy matching
    for brand_data in brands:
        score = fuzzy_match(company_name, brand_data['brand'])
        if score > best_score:
            best_score = score
            best_match = brand_data

    return best_match

def categorize_company(company_name: str, industry: str, categories: Dict[str, int]) -> Tuple[str, int]:
    """Categorize company based on name and industry"""
    company_lower = company_name.lower() if company_name else ""
    industry_lower = industry.lower() if industry else ""

    # Category mapping rules
    if any(x in company_lower for x in ['amazon', 'ebay', 'wayfair', 'asos']) or 'e-commerce' in industry_lower:
        return 'Retail and E-Commerce', categories.get('Retail and E-Commerce', 0)

    if any(x in company_lower for x in ['coca-cola', 'coke', 'pepsi', 'starbucks']) or 'food' in industry_lower or 'beverage' in industry_lower:
        return 'Food and Beverage', categories.get('Food and Beverage', 0)

    if any(x in company_lower for x in ['estee lauder', 'estée lauder', 'mac cosmetics', 'clinique', 'bobbi brown']) or 'cosmetics' in industry_lower or 'beauty' in industry_lower:
        return 'Beauty and Personal Care', categories.get('Beauty and Personal Care', 0)

    if any(x in company_lower for x in ['nike', 'adidas', 'under armour', 'puma', 'reebok', 'hugo boss']) or 'apparel' in industry_lower or 'fashion' in industry_lower:
        return 'Fashion and Accessories', categories.get('Fashion and Accessories', 0)

    if any(x in company_lower for x in ['ford', 'tesla', 'toyota', 'bmw', 'honda']) or 'automotive' in industry_lower:
        return 'Automotive', categories.get('Automotive', 0)

    if any(x in company_lower for x in ['apple', 'samsung', 'microsoft', 'google', 'hp', 'dell']) or 'electronics' in industry_lower or 'consumer electronics' in industry_lower:
        return 'Electronics and Technology', categories.get('Electronics and Technology', 0)

    if any(x in company_lower for x in ['disney', 'netflix', 'hulu', 'paramount']) or 'entertainment' in industry_lower:
        return 'Entertainment and Streaming', categories.get('Entertainment and Streaming', 0)

    if any(x in company_lower for x in ['t-mobile', 'verizon', 'at&t', 'vodafone']) or 'telecommunications' in industry_lower:
        return 'Telecommunications', categories.get('Telecommunications', 0)

    if 'software' in industry_lower or 'computer software' in industry_lower:
        return 'Software', categories.get('Software', 0)

    if 'retail' in industry_lower:
        return 'Retail and E-Commerce', categories.get('Retail and E-Commerce', 0)

    if any(x in company_lower for x in ['ikea', 'home depot', 'wayfair']) or 'home' in industry_lower or 'furniture' in industry_lower:
        return 'Home and Garden', categories.get('Home and Garden', 0)

    if any(x in company_lower for x in ['pets at home', 'petsmart', 'petco']) or 'pet' in industry_lower:
        return 'Pet Food & Care', categories.get('Pet Food & Care', 0)

    if 'airline' in industry_lower or 'aviation' in industry_lower or any(x in company_lower for x in ['jet2', 'airways']):
        return 'Travel, Tourism and Hospitality', categories.get('Travel, Tourism and Hospitality', 0)

    if 'health' in industry_lower or 'wellness' in industry_lower or 'fitness' in industry_lower:
        return 'Health, Wellness, and Fitness', categories.get('Health, Wellness, and Fitness', 0)

    # Default fallback
    if 'technology' in industry_lower:
        return 'Electronics and Technology', categories.get('Electronics and Technology', 0)

    return 'Consumer Goods (FMCG/CPG)', categories.get('Consumer Goods (FMCG/CPG)', 0)

def extract_company_name(lead: Dict) -> str:
    """Extract company name from lead data"""
    # Primary field for company
    company = lead.get('inline-flex', '').strip()
    if company:
        return company

    # Fallback: extract from title
    title = lead.get('font-qanelas', '').strip()
    if ' at ' in title:
        company = title.split(' at ')[-1].strip()
        return company

    return ''

def extract_industry(lead: Dict) -> str:
    """Extract industry from lead data"""
    # Industry appears in font-qanelas 13 or 14
    industry1 = lead.get('font-qanelas 13', '').strip()
    industry2 = lead.get('font-qanelas 14', '').strip()

    # Return the one that looks like an industry (not a date or location)
    for ind in [industry1, industry2]:
        if ind and '/' not in ind and ',' not in ind and ind != '-':
            # Check if it's not a date
            if not re.match(r'\d+/\d+/\d+', ind):
                return ind

    return ''

def enrich_leads(batch_file: str, pivot_file: str, category_file: str, output_file: str):
    """Main enrichment function"""

    # Load data
    with open(batch_file, 'r', encoding='utf-8') as f:
        batch_data = json.load(f)

    leads = batch_data['leads']
    brands = load_pivot_table(pivot_file)
    categories = load_categories(category_file)

    print(f"Loaded {len(leads)} leads")
    print(f"Loaded {len(brands)} brands from pivot table")
    print(f"Loaded {len(categories)} categories")

    # Enrich each lead
    enriched_leads = []
    matched_count = 0
    top_companies = []

    for lead in leads:
        # Extract company and industry
        company_name = extract_company_name(lead)
        industry = extract_industry(lead)

        # Match to brand
        brand_match = match_brand(company_name, brands)

        # Create enriched record
        enriched = lead.copy()

        if brand_match:
            matched_count += 1
            enriched['brand_in_golden_sheet'] = 'Yes'
            enriched['total_assets_tested'] = brand_match['total_assets']
            enriched['platforms_tested'] = brand_match['platforms_str']
            enriched['markets_tested'] = brand_match['markets']

            # Track top companies
            top_companies.append({
                'company': company_name,
                'assets': brand_match['total_assets'],
                'platforms': brand_match['platforms_str']
            })
        else:
            enriched['brand_in_golden_sheet'] = 'No'
            enriched['total_assets_tested'] = ''
            enriched['platforms_tested'] = ''
            enriched['markets_tested'] = ''

        # Categorize
        category, category_count = categorize_company(company_name, industry, categories)
        enriched['company_category'] = category
        enriched['category_asset_count'] = str(category_count)

        enriched_leads.append(enriched)

    # Write to CSV
    if enriched_leads:
        # Get all unique keys
        all_keys = set()
        for lead in enriched_leads:
            all_keys.update(lead.keys())

        fieldnames = sorted(list(all_keys))

        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(enriched_leads)

    # Print summary
    print(f"\n=== ENRICHMENT SUMMARY ===")
    print(f"Total leads processed: {len(leads)}")
    print(f"Leads matched to Golden Sheet: {matched_count}")
    print(f"Match rate: {matched_count/len(leads)*100:.1f}%")

    # Top 3 companies
    if top_companies:
        # Sort by assets (try to convert to int)
        try:
            top_companies.sort(key=lambda x: int(x['assets']) if x['assets'] else 0, reverse=True)
        except:
            pass

        print(f"\n=== TOP 3 COMPANIES FOUND ===")
        for i, comp in enumerate(top_companies[:3], 1):
            print(f"{i}. {comp['company']}")
            print(f"   - Total assets tested: {comp['assets']}")
            print(f"   - Platforms: {comp['platforms']}")

    print(f"\nOutput saved to: {output_file}")

if __name__ == '__main__':
    enrich_leads(
        batch_file='/home/user/ClaudeCodeTest/agent_batches/enrichment_batch_7.json',
        pivot_file='/home/user/ClaudeCodeTest/Golden Sheet - Pivot Table Brands.csv',
        category_file='/home/user/ClaudeCodeTest/Golden Sheet - Category_Count.csv',
        output_file='/home/user/ClaudeCodeTest/enriched_results/enriched_batch_7.csv'
    )
