import csv

# Read the enriched data
input_file = '/home/user/ClaudeCodeTest/enriched_results/enriched_batch_4.csv'
output_file = '/home/user/ClaudeCodeTest/scored_results/scored_batch_4.csv'

# Scoring function based on ICP criteria
def score_lead(lead):
    """
    Score lead based on:
    - Seniority (30%): Director+ = high, Sr Manager = medium, Manager = lower
    - Company budget signals (25%): Golden Sheet presence is HUGE signal, Fortune 100
    - Category fit (10%): Beauty/Electronics/F&B/Entertainment = best
    - Role focus (15%): Brand/influencer/content in title
    - Golden Sheet data (20%): 50+ assets = 10/10, 20-49 = 7/10, 1-19 = 5/10
    """

    # Map column names
    job_title = lead.get('font-qanelas 8', '').lower()
    headline = lead.get('font-qanelas', '').lower()
    company = lead.get('inline-flex', '')
    category = lead.get('company_category', '')
    in_golden_sheet = lead.get('brand_in_golden_sheet', 'No')
    total_assets = int(lead.get('total_assets_tested', 0)) if lead.get('total_assets_tested') else 0
    category_assets = int(lead.get('category_asset_count', 0)) if lead.get('category_asset_count') else 0

    # Seniority scoring (30%)
    seniority_score = 0
    if any(x in job_title or x in headline for x in ['vp', 'vice president', 'director', 'head of']):
        seniority_score = 9
    elif any(x in job_title or x in headline for x in ['senior manager', 'sr manager', 'sr. manager', 'lead manager', '(lead)']):
        seniority_score = 8
    elif 'senior brand' in job_title or 'sr brand' in job_title or 'sr. brand' in job_title:
        seniority_score = 8
    elif 'manager' in job_title and 'brand' in job_title:
        seniority_score = 6.5
    elif 'manager' in job_title:
        seniority_score = 6
    else:
        seniority_score = 4

    # Company/Golden Sheet scoring (45% combined: 25% company + 20% assets)
    company_score = 0
    asset_score = 0

    # Fortune 100 companies
    fortune_100 = ['apple', 'amazon', 'nike', 'coca-cola', 'disney', 'ford', 'estée lauder', 'estee lauder']

    if in_golden_sheet == 'Yes':
        if total_assets >= 50:
            company_score = 10
            asset_score = 10
        elif total_assets >= 20:
            company_score = 9
            asset_score = 7
        elif total_assets >= 10:
            company_score = 8
            asset_score = 6
        else:
            company_score = 8
            asset_score = 5
    else:
        # Not in Golden Sheet - check if Fortune 100
        if any(f100 in company.lower() for f100 in fortune_100):
            company_score = 4
            asset_score = 0
        else:
            company_score = 3
            asset_score = 0

    # Category fit scoring (10%)
    category_score = 0
    if any(cat in category.lower() for cat in ['beauty', 'electronics', 'food and beverage', 'entertainment']):
        category_score = 10
    elif any(cat in category.lower() for cat in ['fashion', 'accessories']):
        category_score = 10
    elif any(cat in category.lower() for cat in ['automotive', 'health', 'wellness']):
        category_score = 6
    else:
        category_score = 5

    # Role focus scoring (15%)
    role_score = 0
    if 'brand' in job_title and 'marketing' in job_title:
        role_score = 10
    elif 'brand' in job_title and 'manager' in job_title:
        role_score = 10
    elif 'influencer' in headline:
        role_score = 10
    elif 'brand' in job_title:
        role_score = 8
    elif any(x in job_title for x in ['marketing', 'innovation lab']):
        role_score = 7
    else:
        role_score = 5

    # Calculate weighted score
    final_score = (
        seniority_score * 0.30 +
        company_score * 0.25 +
        asset_score * 0.20 +
        role_score * 0.15 +
        category_score * 0.10
    )

    return round(final_score, 1), seniority_score, company_score, asset_score, category_score, role_score

# Generate reasoning
def generate_reasoning(lead, scores):
    score, seniority, company, assets, category, role = scores

    job_title = lead.get('font-qanelas 8', '')
    company_name = lead.get('inline-flex', '')
    in_golden_sheet = lead.get('brand_in_golden_sheet', 'No')
    total_assets = lead.get('total_assets_tested', '0')
    category_name = lead.get('company_category', '')

    # Determine seniority level
    if seniority >= 8:
        seniority_desc = "Senior/Director-level"
    elif seniority >= 6:
        seniority_desc = "Mid-level manager"
    else:
        seniority_desc = "Entry/Junior-level"

    # Golden Sheet presence
    if in_golden_sheet == 'Yes':
        golden_desc = f"IN Golden Sheet with {total_assets} tested assets (major signal)"
    else:
        golden_desc = "NOT in Golden Sheet (significant gap)"

    # Category fit
    if category_name in ['Beauty and Personal Care', 'Electronics and Technology', 'Food and Beverage', 'Entertainment and Streaming', 'Fashion and Accessories']:
        category_desc = f"perfect category fit ({category_name})"
    else:
        category_desc = f"moderate category fit ({category_name})"

    reasoning = f"{seniority_desc} brand role at {company_name}. {golden_desc}. Strong {category_desc} with clear brand marketing focus."

    return reasoning[:250]  # Limit to 250 chars

# Process all leads
with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    leads = list(reader)

# Add new columns
new_fieldnames = list(fieldnames) + ['icp_score', 'score_reasoning']

# Score each lead
scored_leads = []
for lead in leads:
    scores = score_lead(lead)
    lead['icp_score'] = scores[0]
    lead['score_reasoning'] = generate_reasoning(lead, scores)
    scored_leads.append(lead)

# Write output
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=new_fieldnames)
    writer.writeheader()
    writer.writerows(scored_leads)

# Generate summary stats
scores = [float(lead['icp_score']) for lead in scored_leads]
high_scores = [s for s in scores if s >= 8]
mid_scores = [s for s in scores if 6 <= s < 8]
low_scores = [s for s in scores if 4 <= s < 6]
very_low_scores = [s for s in scores if s < 4]

# Find top 3
sorted_leads = sorted(scored_leads, key=lambda x: float(x['icp_score']), reverse=True)
top_3 = sorted_leads[:3]

print(f"\n{'='*60}")
print(f"BATCH 4 SCORING SUMMARY")
print(f"{'='*60}")
print(f"\nTotal leads processed: {len(scored_leads)}")
print(f"\nScore Distribution:")
print(f"  8.0+ (Hot leads):        {len(high_scores)} leads")
print(f"  6.0-7.9 (Warm leads):    {len(mid_scores)} leads")
print(f"  4.0-5.9 (Cold leads):    {len(low_scores)} leads")
print(f"  <4.0 (Poor fit):         {len(very_low_scores)} leads")

print(f"\nTop 3 Highest Scoring Leads:")
print(f"{'-'*60}")
for i, lead in enumerate(top_3, 1):
    print(f"{i}. {lead['truncate']} - Score: {lead['icp_score']}")
    print(f"   Company: {lead['inline-flex']}")
    print(f"   Title: {lead['font-qanelas 12']}")
    print(f"   Golden Sheet: {lead['brand_in_golden_sheet']} ({lead.get('total_assets_tested', 0)} assets)")
    print(f"   Reasoning: {lead['score_reasoning']}")
    print()

print(f"{'='*60}")
print(f"Output saved to: {output_file}")
print(f"{'='*60}\n")
