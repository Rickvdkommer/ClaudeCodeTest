#!/usr/bin/env python3
"""
Simple consolidation of scored batch results
"""

import pandas as pd
from pathlib import Path

# Configuration
SCORED_DIR = Path("scored_results")
FINAL_OUTPUT = "Leads_Final_Enriched_and_Scored.csv"

def consolidate():
    print("=" * 80)
    print("CONSOLIDATING SCORED RESULTS")
    print("=" * 80)

    # Find all scored batch files
    batch_files = sorted(SCORED_DIR.glob("scored_batch_*.csv"))

    print(f"\nFound {len(batch_files)} scored batch files")

    # Read and concatenate all batches
    all_dfs = []
    for batch_file in batch_files:
        df = pd.read_csv(batch_file)
        all_dfs.append(df)
        print(f"  ✓ Loaded {batch_file.name}: {len(df)} leads")

    # Combine all dataframes
    final_df = pd.concat(all_dfs, ignore_index=True)

    # Write to final file
    final_df.to_csv(FINAL_OUTPUT, index=False)

    print(f"\n✅ Consolidated {len(final_df)} leads to {FINAL_OUTPUT}")

    # Statistics
    print("\n" + "=" * 80)
    print("FINAL STATISTICS")
    print("=" * 80)

    print(f"\n📊 OVERALL:")
    print(f"  Total Leads: {len(final_df)}")

    # Golden Sheet stats
    golden_sheet_count = (final_df['brand_in_golden_sheet'] == 'Yes').sum()
    print(f"  In Golden Sheet: {golden_sheet_count} ({golden_sheet_count/len(final_df)*100:.1f}%)")

    # Score distribution
    print(f"\n🎯 SCORE DISTRIBUTION:")
    score_bins = [
        (9.0, 10.0, '9.0-10.0 (Exceptional)'),
        (8.0, 8.99, '8.0-8.9 (Hot)'),
        (7.0, 7.99, '7.0-7.9 (Strong)'),
        (6.0, 6.99, '6.0-6.9 (Good)'),
        (5.0, 5.99, '5.0-5.9 (Moderate)'),
        (0, 4.99, '<5.0 (Weak)')
    ]

    for min_score, max_score, label in score_bins:
        count = ((final_df['icp_score'] >= min_score) & (final_df['icp_score'] <= max_score)).sum()
        pct = count / len(final_df) * 100
        bar = "█" * int(pct / 2)
        print(f"  {label:25s}: {count:3d} ({pct:5.1f}%) {bar}")

    # Score stats
    print(f"\n📈 SCORE STATISTICS:")
    print(f"  Average: {final_df['icp_score'].mean():.2f}")
    print(f"  Median: {final_df['icp_score'].median():.2f}")
    print(f"  Highest: {final_df['icp_score'].max():.2f}")
    print(f"  Lowest: {final_df['icp_score'].min():.2f}")

    # Top companies
    print(f"\n🏆 TOP 10 COMPANIES:")
    company_col = final_df.columns[8]  # Company column
    top_companies = final_df[company_col].value_counts().head(10)
    for i, (company, count) in enumerate(top_companies.items(), 1):
        print(f"  {i:2d}. {company}: {count} leads")

    # Top categories
    print(f"\n📁 TOP 10 CATEGORIES:")
    top_categories = final_df['company_category'].value_counts().head(10)
    for i, (category, count) in enumerate(top_categories.items(), 1):
        pct = count / len(final_df) * 100
        print(f"  {i:2d}. {category}: {count} leads ({pct:.1f}%)")

    # High-value leads
    high_value = (final_df['icp_score'] >= 8.0).sum()
    print(f"\n💎 HIGH-VALUE LEADS (8.0+): {high_value} ({high_value/len(final_df)*100:.1f}%)")

    print("\n" + "=" * 80)
    print(f"✅ SUCCESS! Final file: {FINAL_OUTPUT}")
    print("=" * 80)

if __name__ == "__main__":
    consolidate()
