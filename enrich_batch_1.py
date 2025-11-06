#!/usr/bin/env python3
import json
import csv
import os
from difflib import SequenceMatcher

def fuzzy_match(str1, str2, threshold=0.7):
    """Calculate similarity between two strings"""
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio() >= threshold

def normalize_brand_name(name):
    """Normalize brand names for matching"""
    # Remove common suffixes and special characters
    name = name.lower().strip()
    name = name.replace('&', 'and')
    name = name.replace(',', '')
    name = name.replace('.', '')
    name = name.replace('  ', ' ')
    return name

def find_brand_match(company_name, pivot_data):
    """Find matching brand in pivot table with fuzzy matching"""
    company_norm = normalize_brand_name(company_name)

    # Special brand name mappings
    brand_mappings = {
        'apple': ['apple', 'アップル'],
        'disney': ['disney', 'ディズニー'],
        'coca-cola': ['coca cola', 'coca-cola', 'coke', 'コカコーラ'],
        'samsung': ['samsung', '삼성'],
        'nike': ['nike', 'ナイキ'],
        'amazon': ['amazon'],
        'prime video': ['amazon_prime', 'prime video and amazon mgm studios', 'prime video'],
        'ford': ['ford', 'フォード'],
        't-mobile': ['t-mobile', 'tmobile', 'metro by t-mobile'],
    }

    # Check if company matches any known brand
    for canonical_brand, variants in brand_mappings.items():
        if any(variant in company_norm for variant in variants):
            company_norm = canonical_brand

    # Try exact match first
    for brand_row in pivot_data:
        brand_name = brand_row.get('Main Brand', '').strip()
        if not brand_name:
            continue

        brand_norm = normalize_brand_name(brand_name)

        # Exact match
        if company_norm == brand_norm:
            return brand_row

        # Check if one contains the other
        if company_norm in brand_norm or brand_norm in company_norm:
            return brand_row

        # Check brand mappings
        for canonical_brand, variants in brand_mappings.items():
            if any(variant in brand_norm for variant in variants):
                if canonical_brand == company_norm:
                    return brand_row

    # Try fuzzy matching
    best_match = None
    best_score = 0.7  # minimum threshold

    for brand_row in pivot_data:
        brand_name = brand_row.get('Main Brand', '').strip()
        if not brand_name:
            continue

        brand_norm = normalize_brand_name(brand_name)

        # Handle special cases
        # Amazon variants
        if 'amazon' in company_norm:
            if 'amazon' in brand_norm or 'prime' in brand_norm:
                score = SequenceMatcher(None, company_norm, brand_norm).ratio()
                if score > best_score:
                    best_score = score
                    best_match = brand_row

        # T-Mobile variants
        elif 't-mobile' in company_norm or 'tmobile' in company_norm or 'metro by t-mobile' in company_norm:
            if 't-mobile' in brand_norm or 'metro by t-mobile' in brand_norm:
                return brand_row

        # General fuzzy matching
        else:
            score = SequenceMatcher(None, company_norm, brand_norm).ratio()
            if score > best_score:
                best_score = score
                best_match = brand_row

    return best_match

def categorize_company(company_name, industry, categories):
    """Categorize company based on name and industry"""
    company_lower = company_name.lower()
    industry_lower = industry.lower() if industry else ""

    # Mapping rules based on industry and company name
    if 'food' in industry_lower or 'beverage' in industry_lower:
        return 'Food and Beverage'
    elif 'retail' in industry_lower or 'petsmart' in company_lower:
        if 'pet' in company_lower or 'petsmart' in company_lower:
            return 'Pet Food & Care'
        return 'Retail and E-Commerce'
    elif 'entertainment' in industry_lower:
        return 'Entertainment and Streaming'
    elif 'computer software' in industry_lower or 'information technology' in industry_lower:
        if 'amazon' in company_lower:
            return 'Retail and E-Commerce'
        return 'Software'
    elif 'telecommunications' in industry_lower:
        return 'Telecommunications'
    elif 'consumer electronics' in industry_lower:
        return 'Electronics and Technology'
    elif 'cosmetics' in industry_lower or 'estée lauder' in company_lower or 'mac cosmetics' in company_lower:
        return 'Beauty and Personal Care'
    elif 'travel' in industry_lower or 'tourism' in industry_lower or 'viator' in company_lower:
        return 'Travel, Tourism and Hospitality'
    elif 'automotive' in industry_lower or 'ford' in company_lower:
        return 'Automotive'
    elif 'restaurant' in industry_lower:
        return 'QSR (Quick Service Restaurants)'
    elif 'hospital' in industry_lower or 'health care' in industry_lower or 'philips' in company_lower:
        if 'philips' in company_lower:
            return 'Electronics and Technology'
        return 'Health, Wellness, and Fitness'
    elif 'nike' in company_lower:
        return 'Fashion and Accessories'
    elif 'disney' in company_lower:
        return 'Entertainment and Streaming'
    elif 'apple' in company_lower:
        return 'Electronics and Technology'
    elif 'samsung' in company_lower:
        return 'Electronics and Technology'
    elif 'electrical' in industry_lower or 'electronic manufacturing' in industry_lower:
        return 'Electronics and Technology'
    elif 'marketing' in industry_lower or 'advertising' in industry_lower:
        return 'Services (Professional and Consumer)'
    elif 'ebay' in company_lower:
        return 'Retail and E-Commerce'

    # Default to a general category
    return 'Services (Professional and Consumer)'

def get_category_asset_count(category, category_data):
    """Get asset count for a category"""
    for cat_row in category_data:
        if cat_row['Primary Category'] == category:
            return cat_row['Number of Assets Tested']
    return ''

def main():
    # Read batch JSON file
    with open('/home/user/ClaudeCodeTest/agent_batches/enrichment_batch_1.json', 'r') as f:
        batch_data = json.load(f)

    # Read category count CSV
    category_data = []
    with open('/home/user/ClaudeCodeTest/Golden Sheet - Category_Count.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        category_data = list(reader)

    # Read pivot table CSV - skip first 3 header rows
    pivot_data = []
    with open('/home/user/ClaudeCodeTest/Golden Sheet - Pivot Table Brands.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
        # Skip first 3 rows, process from row 4 onwards
        for row in rows[3:]:
            if len(row) >= 10 and row[1]:  # Must have brand name
                brand_row = {
                    'Main Brand': row[1],
                    'amazon_prime': row[2] if len(row) > 2 else '',
                    'instagram': row[3] if len(row) > 3 else '',
                    'netflix': row[4] if len(row) > 4 else '',
                    'standalone': row[5] if len(row) > 5 else '',
                    'tiktok': row[6] if len(row) > 6 else '',
                    'youtube_shorts': row[7] if len(row) > 7 else '',
                    'Grand Total': row[8] if len(row) > 8 else '',
                    'platforms_list': row[9] if len(row) > 9 else '',
                    'markets_list': row[10] if len(row) > 10 else ''
                }
                pivot_data.append(brand_row)

    # Process leads
    enriched_leads = []
    matched_count = 0
    top_companies = []

    for lead in batch_data['leads']:
        # Extract company name
        company_name = lead.get('inline-flex', '')

        # Get industry from available fields
        industry = lead.get('font-qanelas 14') or lead.get('font-qanelas 13', '')

        # Find brand match
        brand_match = find_brand_match(company_name, pivot_data)

        # Create enriched lead with all original fields
        enriched_lead = lead.copy()

        if brand_match:
            matched_count += 1
            enriched_lead['brand_in_golden_sheet'] = 'Yes'
            enriched_lead['total_assets_tested'] = brand_match.get('Grand Total', '')

            # Extract platforms (non-empty platform columns)
            platforms = []
            for platform in ['amazon_prime', 'instagram', 'netflix', 'standalone', 'tiktok', 'youtube_shorts']:
                if brand_match.get(platform, '').strip():
                    platforms.append(platform)
            enriched_lead['platforms_tested'] = ', '.join(platforms) if platforms else ''

            # Get markets from the markets_list field
            enriched_lead['markets_tested'] = brand_match.get('markets_list', '')

            # Track top companies
            total_assets = brand_match.get('Grand Total', '0')
            try:
                total_assets_int = int(total_assets) if total_assets else 0
            except:
                total_assets_int = 0

            top_companies.append({
                'company': company_name,
                'total_assets': total_assets_int,
                'platforms': enriched_lead['platforms_tested'],
                'markets': enriched_lead['markets_tested']
            })
        else:
            enriched_lead['brand_in_golden_sheet'] = 'No'
            enriched_lead['total_assets_tested'] = ''
            enriched_lead['platforms_tested'] = ''
            enriched_lead['markets_tested'] = ''

        # Categorize company
        category = categorize_company(company_name, industry, category_data)
        enriched_lead['company_category'] = category
        enriched_lead['category_asset_count'] = get_category_asset_count(category, category_data)

        enriched_leads.append(enriched_lead)

    # Sort top companies by total assets
    top_companies.sort(key=lambda x: x['total_assets'], reverse=True)
    top_3 = top_companies[:3]

    # Create output directory if it doesn't exist
    os.makedirs('/home/user/ClaudeCodeTest/enriched_results', exist_ok=True)

    # Write enriched data to CSV
    if enriched_leads:
        # Get all field names (original + new ones)
        fieldnames = list(enriched_leads[0].keys())

        with open('/home/user/ClaudeCodeTest/enriched_results/enriched_batch_1.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(enriched_leads)

    # Print summary
    print(f"ENRICHMENT SUMMARY - BATCH 1")
    print(f"=" * 60)
    print(f"Total leads processed: {len(enriched_leads)}")
    print(f"Leads matched to Golden Sheet brands: {matched_count}")
    print(f"Match rate: {matched_count/len(enriched_leads)*100:.1f}%")
    print()
    print(f"TOP 3 COMPANIES WITH GOLDEN SHEET DATA:")
    print(f"-" * 60)
    for i, company in enumerate(top_3, 1):
        print(f"\n{i}. {company['company']}")
        print(f"   Total Assets Tested: {company['total_assets']}")
        print(f"   Platforms: {company['platforms']}")
        print(f"   Markets: {company['markets']}")

    print(f"\n" + "=" * 60)
    print(f"Output saved to: /home/user/ClaudeCodeTest/enriched_results/enriched_batch_1.csv")

if __name__ == '__main__':
    main()
