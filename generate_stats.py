#!/usr/bin/env python3
"""
Generate statistics for final scored CSV
"""

import csv
from collections import Counter, defaultdict

FINAL_FILE = "Leads_Final_Enriched_and_Scored.csv"

print("=" * 80)
print("FINAL RESULTS ANALYSIS")
print("=" * 80)

# Read the final CSV
with open(FINAL_FILE, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    leads = list(reader)

print(f"\n✅ Loaded {len(leads)} leads from {FINAL_FILE}")

# Collect statistics
scores = []
golden_sheet_count = 0
categories = Counter()
companies = Counter()
score_dist = defaultdict(int)

for lead in leads:
    # Score
    try:
        score = float(lead['icp_score'])
        scores.append(score)

        # Distribution
        if score >= 9.0:
            score_dist['9.0-10.0'] += 1
        elif score >= 8.0:
            score_dist['8.0-8.9'] += 1
        elif score >= 7.0:
            score_dist['7.0-7.9'] += 1
        elif score >= 6.0:
            score_dist['6.0-6.9'] += 1
        elif score >= 5.0:
            score_dist['5.0-5.9'] += 1
        else:
            score_dist['<5.0'] += 1
    except:
        pass

    # Golden Sheet
    if lead.get('brand_in_golden_sheet', '').strip().lower() == 'yes':
        golden_sheet_count += 1

    # Category
    cat = lead.get('company_category', 'Unknown')
    categories[cat] += 1

    # Company - using column index 8
    company = lead.get('font-qanelas 8', 'Unknown')
    companies[company] += 1

# Sort scores
scores.sort(reverse=True)

# Print statistics
print("\n" + "=" * 80)
print("📊 OVERALL METRICS")
print("=" * 80)
print(f"Total Leads: {len(leads)}")
print(f"In Golden Sheet: {golden_sheet_count} ({golden_sheet_count/len(leads)*100:.1f}%)")
print(f"Not in Golden Sheet: {len(leads) - golden_sheet_count} ({(len(leads) - golden_sheet_count)/len(leads)*100:.1f}%)")

print("\n" + "=" * 80)
print("🎯 ICP SCORE DISTRIBUTION")
print("=" * 80)

score_ranges = ['9.0-10.0', '8.0-8.9', '7.0-7.9', '6.0-6.9', '5.0-5.9', '<5.0']
for range_name in score_ranges:
    count = score_dist[range_name]
    pct = count / len(scores) * 100
    bar = "█" * int(pct / 2)
    priority = ""
    if range_name in ['9.0-10.0', '8.0-8.9']:
        priority = " 🔥 HOT"
    elif range_name in ['7.0-7.9']:
        priority = " ✅ STRONG"
    elif range_name in ['6.0-6.9']:
        priority = " ⚠️ MODERATE"

    print(f"{range_name}: {count:3d} leads ({pct:5.1f}%) {bar}{priority}")

print("\n" + "=" * 80)
print("📈 SCORE STATISTICS")
print("=" * 80)
print(f"Average Score: {sum(scores)/len(scores):.2f}")
print(f"Median Score: {scores[len(scores)//2]:.2f}")
print(f"Highest Score: {max(scores):.2f}")
print(f"Lowest Score: {min(scores):.2f}")

high_value = sum(1 for s in scores if s >= 8.0)
strong_value = sum(1 for s in scores if s >= 7.0)

print(f"\n💎 High-Value Leads (8.0+): {high_value} ({high_value/len(scores)*100:.1f}%)")
print(f"💎 Strong+ Leads (7.0+): {strong_value} ({strong_value/len(scores)*100:.1f}%)")

print("\n" + "=" * 80)
print("🏆 TOP 10 COMPANIES BY LEAD COUNT")
print("=" * 80)
for i, (company, count) in enumerate(companies.most_common(10), 1):
    print(f"{i:2d}. {company}: {count} leads")

print("\n" + "=" * 80)
print("📁 TOP 10 CATEGORIES")
print("=" * 80)
for i, (category, count) in enumerate(categories.most_common(10), 1):
    pct = count / len(leads) * 100
    print(f"{i:2d}. {category}: {count} leads ({pct:.1f}%)")

print("\n" + "=" * 80)
print("🎉 ANALYSIS COMPLETE!")
print("=" * 80)
print(f"\nFinal enriched and scored file: {FINAL_FILE}")
print(f"All {len(leads)} leads ready for outreach!")
print("\nColumns included:")
print("  - All original lead data")
print("  - brand_in_golden_sheet, total_assets_tested, platforms_tested, markets_tested")
print("  - company_category, category_asset_count")
print("  - icp_score (1-10), score_reasoning")
