#!/usr/bin/env python3
"""
Consolidate all scored batch results into final CSV with statistics
"""

import csv
import os
from pathlib import Path

# Configuration
SCORED_DIR = Path("scored_results")
FINAL_OUTPUT = "Leads_Final_Enriched_and_Scored.csv"

def consolidate_results():
    """Consolidate all scored batch files into one final CSV"""

    print("=" * 80)
    print("CONSOLIDATING SCORED RESULTS")
    print("=" * 80)

    # Find all scored batch files
    batch_files = sorted(SCORED_DIR.glob("scored_batch_*.csv"))

    if not batch_files:
        print("ERROR: No scored batch files found!")
        return

    print(f"\nFound {len(batch_files)} scored batch files:")
    for f in batch_files:
        size_kb = f.stat().st_size / 1024
        print(f"  - {f.name} ({size_kb:.1f} KB)")

    # Read all batches and collect all unique headers
    all_leads = []
    all_headers = set()
    stats = {
        'total_leads': 0,
        'scores': [],
        'in_golden_sheet': 0,
        'categories': {},
        'companies': {}
    }

    for batch_file in batch_files:
        with open(batch_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Collect all headers across all files
            all_headers.update(reader.fieldnames)

            # Read all rows
            batch_leads = list(reader)
            all_leads.extend(batch_leads)

    # Convert headers set to sorted list for consistency
    headers = sorted(all_headers)

    # Re-read all batches with standardized headers
    all_leads = []
    for batch_file in batch_files:
        with open(batch_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            batch_leads = list(reader)
            all_leads.extend(batch_leads)

            # Collect statistics
            for lead in batch_leads:
                stats['total_leads'] += 1

                # Score distribution
                try:
                    score = float(lead.get('icp_score', 0))
                    stats['scores'].append(score)
                except:
                    pass

                # Golden sheet count
                if lead.get('brand_in_golden_sheet', '').lower() == 'yes':
                    stats['in_golden_sheet'] += 1

                # Category distribution
                category = lead.get('company_category', 'Unknown')
                stats['categories'][category] = stats['categories'].get(category, 0) + 1

                # Company distribution
                company = lead.get('font-qanelas 8', 'Unknown')  # Company name column
                stats['companies'][company] = stats['companies'].get(company, 0) + 1

    # Write consolidated file
    print(f"\n[1/3] Writing consolidated file...")
    with open(FINAL_OUTPUT, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(all_leads)

    file_size = Path(FINAL_OUTPUT).stat().st_size / 1024
    print(f"  ✓ Wrote {stats['total_leads']} leads to {FINAL_OUTPUT} ({file_size:.1f} KB)")

    # Calculate statistics
    print(f"\n[2/3] Calculating statistics...")
    scores = stats['scores']
    scores.sort(reverse=True)

    score_dist = {
        '9.0-10.0': sum(1 for s in scores if s >= 9.0),
        '8.0-8.9': sum(1 for s in scores if 8.0 <= s < 9.0),
        '7.0-7.9': sum(1 for s in scores if 7.0 <= s < 8.0),
        '6.0-6.9': sum(1 for s in scores if 6.0 <= s < 7.0),
        '5.0-5.9': sum(1 for s in scores if 5.0 <= s < 6.0),
        '<5.0': sum(1 for s in scores if s < 5.0),
    }

    # Print statistics
    print(f"\n[3/3] FINAL STATISTICS")
    print("=" * 80)

    print(f"\n📊 OVERALL METRICS:")
    print(f"  Total Leads: {stats['total_leads']}")
    print(f"  In Golden Sheet: {stats['in_golden_sheet']} ({stats['in_golden_sheet']/stats['total_leads']*100:.1f}%)")
    print(f"  Not in Golden Sheet: {stats['total_leads'] - stats['in_golden_sheet']} ({(stats['total_leads'] - stats['in_golden_sheet'])/stats['total_leads']*100:.1f}%)")

    print(f"\n🎯 SCORE DISTRIBUTION:")
    for range_name, count in score_dist.items():
        pct = count / len(scores) * 100
        bar = "█" * int(pct / 2)
        print(f"  {range_name}: {count:3d} leads ({pct:5.1f}%) {bar}")

    print(f"\n📈 SCORE STATISTICS:")
    print(f"  Average: {sum(scores)/len(scores):.2f}")
    print(f"  Median: {scores[len(scores)//2]:.2f}")
    print(f"  Highest: {max(scores):.2f}")
    print(f"  Lowest: {min(scores):.2f}")

    print(f"\n🏆 TOP 10 COMPANIES BY LEAD COUNT:")
    top_companies = sorted(stats['companies'].items(), key=lambda x: x[1], reverse=True)[:10]
    for i, (company, count) in enumerate(top_companies, 1):
        print(f"  {i:2d}. {company}: {count} leads")

    print(f"\n📁 TOP 10 CATEGORIES:")
    top_categories = sorted(stats['categories'].items(), key=lambda x: x[1], reverse=True)[:10]
    for i, (category, count) in enumerate(top_categories, 1):
        pct = count / stats['total_leads'] * 100
        print(f"  {i:2d}. {category}: {count} leads ({pct:.1f}%)")

    print("\n" + "=" * 80)
    print("✅ CONSOLIDATION COMPLETE!")
    print("=" * 80)
    print(f"\nFinal output: {FINAL_OUTPUT}")
    print(f"All {stats['total_leads']} leads enriched and scored!")

if __name__ == "__main__":
    consolidate_results()
