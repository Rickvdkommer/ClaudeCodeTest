#!/usr/bin/env python3
"""
Agent Orchestration Script for Lead Enrichment and Scoring

This script:
1. Splits leads into batches
2. Launches parallel enrichment agents (add golden sheet data)
3. Launches parallel scoring agents (web research + ICP scoring)
4. Consolidates results into final CSV
"""

import csv
import json
import math
from pathlib import Path

# Configuration
LEADS_FILE = "Exports_Leads_BrandManager.csv"
CATEGORY_FILE = "Golden Sheet - Category_Count.csv"
PIVOT_FILE = "Golden Sheet - Pivot Table Brands.csv"
OUTPUT_DIR = Path("agent_batches")
ENRICHED_DIR = Path("enriched_results")
SCORED_DIR = Path("scored_results")
FINAL_OUTPUT = "Leads_Enriched_and_Scored.csv"
NUM_AGENTS = 8

def load_csv(filename):
    """Load CSV file and return rows"""
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

def create_batches(leads, num_batches):
    """Split leads into equal batches"""
    batch_size = math.ceil(len(leads) / num_batches)
    batches = []

    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, len(leads))
        batch = leads[start_idx:end_idx]
        if batch:  # Only add non-empty batches
            batches.append({
                'batch_num': i + 1,
                'leads': batch,
                'start_idx': start_idx,
                'end_idx': end_idx
            })

    return batches

def save_batch_files(batches, output_dir, prefix="batch"):
    """Save batches as individual JSON files"""
    output_dir.mkdir(exist_ok=True)
    batch_files = []

    for batch in batches:
        filename = output_dir / f"{prefix}_{batch['batch_num']}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(batch, f, indent=2)
        batch_files.append(filename)

    return batch_files

def main():
    print("=" * 80)
    print("LEAD ENRICHMENT AND SCORING ORCHESTRATION")
    print("=" * 80)

    # Load data
    print("\n[1/6] Loading data files...")
    leads = load_csv(LEADS_FILE)
    categories = load_csv(CATEGORY_FILE)
    pivot_data = load_csv(PIVOT_FILE)

    print(f"  ✓ Loaded {len(leads)} leads")
    print(f"  ✓ Loaded {len(categories)} categories")
    print(f"  ✓ Loaded {len(pivot_data)} brands from pivot table")

    # Create batches
    print(f"\n[2/6] Creating {NUM_AGENTS} batches...")
    batches = create_batches(leads, NUM_AGENTS)
    print(f"  ✓ Created {len(batches)} batches")
    for batch in batches:
        print(f"    - Batch {batch['batch_num']}: {len(batch['leads'])} leads (rows {batch['start_idx']}-{batch['end_idx']})")

    # Save batch files
    print(f"\n[3/6] Saving batch files...")
    batch_files = save_batch_files(batches, OUTPUT_DIR, prefix="enrichment_batch")
    print(f"  ✓ Saved {len(batch_files)} batch files to {OUTPUT_DIR}/")

    # Create directories for results
    ENRICHED_DIR.mkdir(exist_ok=True)
    SCORED_DIR.mkdir(exist_ok=True)

    print(f"\n[4/6] READY TO LAUNCH AGENTS")
    print("=" * 80)
    print("\nPHASE 1: ENRICHMENT AGENTS")
    print("  Task: Match companies to golden sheet data")
    print("  Agents needed: 8 parallel agents")
    print(f"  Input: {OUTPUT_DIR}/enrichment_batch_[1-8].json")
    print(f"  Output: {ENRICHED_DIR}/enriched_batch_[1-8].csv")
    print("\nPHASE 2: SCORING AGENTS")
    print("  Task: Web research + comprehensive ICP scoring")
    print("  Agents needed: 8 parallel agents")
    print(f"  Input: {ENRICHED_DIR}/enriched_batch_[1-8].csv")
    print(f"  Output: {SCORED_DIR}/scored_batch_[1-8].csv")
    print("\n" + "=" * 80)

    # Generate agent instructions
    print("\n[5/6] Generating agent instruction files...")

    # Enrichment agent instructions
    enrichment_instructions = f"""
# ENRICHMENT AGENT INSTRUCTIONS

## Your Task
Process your assigned batch of leads and enrich each row with Golden Sheet data.

## Input Files
- Your batch: agent_batches/enrichment_batch_[YOUR_NUMBER].json
- Category data: {CATEGORY_FILE}
- Pivot table: {PIVOT_FILE}

## For Each Lead
1. Extract company name
2. Match company to brand in pivot table (fuzzy matching allowed)
3. If match found, add these columns:
   - brand_in_golden_sheet: Yes/No
   - total_assets_tested: number from Grand Total column
   - platforms_tested: comma-separated list
   - markets_tested: comma-separated list
   - platform_breakdown: JSON with counts per platform
4. Categorize company into one of 29 categories (use best judgment)
5. Add these columns:
   - company_category: category name
   - category_asset_count: number from category count CSV

## Output
Save as CSV: enriched_results/enriched_batch_[YOUR_NUMBER].csv
Include ALL original columns plus new enrichment columns.

## Matching Tips
- Use fuzzy matching (e.g., "Amazon Prime Video" matches "amazon_prime")
- Handle variations (T-Mobile, TMobile, Metro by T-Mobile)
- If no exact match, use closest match or "No"
- For categories, infer from company name and industry field
"""

    with open("ENRICHMENT_AGENT_INSTRUCTIONS.md", 'w') as f:
        f.write(enrichment_instructions)

    # Scoring agent instructions
    scoring_instructions = f"""
# SCORING AGENT INSTRUCTIONS

## Your Task
Score each enriched lead against the ICP using web research and all available data.

## Input Files
- Your batch: enriched_results/enriched_batch_[YOUR_NUMBER].csv

## For Each Lead
1. Web search: "[Person Name] [Company] brand marketing"
2. Web search: "[Company] influencer campaigns marketing budget"
3. Analyze all available data:
   - Job title and seniority
   - Company size and budget signals
   - Golden sheet presence (is brand tested? how many assets?)
   - Category strength (high asset count = mature category)
   - Professional headline keywords
   - LinkedIn activity signals (if findable)

## ICP Scoring Criteria (1-10 scale)

### Score 9-10 (Perfect Fit)
- VP/Director/Head level at Fortune 100 brand
- Company in golden sheet with 50+ assets tested
- High-value category (Beauty, Electronics, Food & Beverage)
- Clear brand marketing/influencer focus in title
- Evidence of campaign activity or measurement needs

### Score 7-8 (Strong Fit)
- Senior Manager at major brand
- Company in golden sheet OR large recognized brand
- Good category fit
- Brand marketing focus evident
- Budget authority likely

### Score 5-6 (Moderate Fit)
- Manager level at mid-size brand
- Company may not be in golden sheet but decent size
- Relevant industry
- Some brand marketing involvement

### Score 3-4 (Weak Fit)
- Junior level or unclear authority
- Small company or pure performance marketing
- Poor category fit
- Limited brand building focus

### Score 1-2 (Poor Fit)
- Entry level
- Tiny budget
- Wrong focus area (pure B2B, performance only)
- No alignment with ICP

## Output Columns
Add these columns:
- icp_score: 1-10 (one decimal, e.g., 7.5)
- score_reasoning: 2-3 sentence explanation covering:
  - Seniority and role fit
  - Company size/budget signals
  - Golden sheet presence (key factor!)
  - Category strength
  - Any web research findings
  - Why this score was assigned

## Output
Save as CSV: scored_results/scored_batch_[YOUR_NUMBER].csv
Include ALL columns from enriched CSV plus score columns.
"""

    with open("SCORING_AGENT_INSTRUCTIONS.md", 'w') as f:
        f.write(scoring_instructions)

    print("  ✓ Created ENRICHMENT_AGENT_INSTRUCTIONS.md")
    print("  ✓ Created SCORING_AGENT_INSTRUCTIONS.md")

    print("\n[6/6] SETUP COMPLETE!")
    print("=" * 80)
    print("\n✓ All batch files created")
    print("✓ Agent instructions ready")
    print("\nNext steps:")
    print("  1. Launch 8 enrichment agents in parallel")
    print("  2. Once enrichment complete, launch 8 scoring agents in parallel")
    print("  3. Run consolidation script to merge all results")
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
