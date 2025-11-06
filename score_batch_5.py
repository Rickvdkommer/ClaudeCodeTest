import csv

# Read the enriched data
input_file = '/home/user/ClaudeCodeTest/enriched_results/enriched_batch_5.csv'
output_file = '/home/user/ClaudeCodeTest/scored_results/scored_batch_5.csv'

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

    # Get columns dynamically
    cols = list(lead.keys())
    name = lead.get(cols[2], '') if len(cols) > 2 else ''
    headline = lead.get(cols[5], '').lower() if len(cols) > 5 else ''
    company = lead.get(cols[8], '') if len(cols) > 8 else ''
    job_title = lead.get(cols[10], '').lower() if len(cols) > 10 else ''

    category = lead.get('company_category', '')
    in_golden_sheet = lead.get('brand_in_golden_sheet', 'No')
    total_assets = int(lead.get('total_assets_tested', 0)) if lead.get('total_assets_tested') else 0
    category_assets = int(lead.get('category_asset_count', 0)) if lead.get('category_asset_count') else 0

    # Seniority scoring (30%) - Budget authority is key
    seniority_score = 0
    if any(x in job_title or x in headline for x in ['vp', 'vice president', 'director', 'head of']):
        seniority_score = 9.0  # High budget authority
    elif any(x in job_title or x in headline for x in ['senior manager', 'sr manager', 'sr. manager']):
        seniority_score = 8.0  # Likely budget authority
    elif 'senior' in job_title or 'sr ' in job_title or 'sr.' in job_title:
        seniority_score = 7.5
    elif 'manager' in job_title or 'manager' in headline:
        seniority_score = 6.5  # Some authority
    elif 'lead' in job_title or 'lead' in headline:
        seniority_score = 7.0
    else:
        seniority_score = 4.5

    # Company/Golden Sheet scoring (45% combined: 25% company + 20% assets)
    # Golden Sheet presence is HUGE signal per ICP
    company_score = 0
    asset_score = 0

    # Tier 1: Companies with verified strong influencer marketing budgets (web research)
    tier1 = ['amazon', 'nike', 'disney', 'coca-cola', 'samsung', 'apple']
    # Tier 2: Major brands with influencer marketing
    tier2 = ['ford', 'philips', 't-mobile', 'directv', 'petsmart', 'sainsbury', 'burger king', 'jeep']

    if in_golden_sheet == 'Yes':
        if total_assets >= 50:
            company_score = 10
            asset_score = 10  # Best signal - 50+ assets
        elif total_assets >= 20:
            company_score = 9
            asset_score = 7
        elif total_assets >= 10:
            company_score = 8.5
            asset_score = 6
        else:
            company_score = 8
            asset_score = 5

        # Boost for tier 1 companies
        if any(t1 in company.lower() for t1 in tier1):
            company_score = min(10, company_score + 0.5)
    else:
        # Not in Golden Sheet - major penalty per ICP
        if any(t1 in company.lower() for t1 in tier1):
            company_score = 5  # Fortune 100 but not in GS
            asset_score = 2
        elif any(t2 in company.lower() for t2 in tier2):
            company_score = 4
            asset_score = 1
        else:
            company_score = 3
            asset_score = 0

    # Category fit scoring (10%)
    category_score = 0
    if any(cat in category.lower() for cat in ['beauty', 'electronics', 'food', 'beverage', 'entertainment']):
        category_score = 10  # Perfect fit
    elif any(cat in category.lower() for cat in ['fashion', 'accessories', 'retail']):
        category_score = 8
    elif any(cat in category.lower() for cat in ['automotive', 'health', 'wellness', 'telecom']):
        category_score = 7
    else:
        category_score = 5

    # Role focus scoring (15%) - Brand marketing focus is critical
    role_score = 0
    # Operational/non-marketing roles
    non_marketing = ['protection', 'registry', 'onboarding', 'event manager', 'technical',
                     'program manager, brand', 'archivist', 'commercialization']

    if 'brand marketing manager' in job_title or 'brand marketing manager' in headline:
        role_score = 10  # Perfect fit
    elif 'brand manager' in job_title:
        # Check if operational
        if any(nm in job_title or nm in headline for nm in non_marketing):
            role_score = 4  # Operational, not marketing
        else:
            role_score = 9.5
    elif 'brand insights' in job_title or 'brand insights' in headline:
        role_score = 7  # Analyst role, less authority
    elif 'brand' in job_title and 'marketing' in job_title:
        role_score = 9.5
    elif 'brand' in job_title:
        if any(nm in job_title or nm in headline for nm in non_marketing):
            role_score = 3.5
        else:
            role_score = 8
    elif 'influencer' in headline or 'creator' in headline:
        role_score = 9
    elif 'marketing' in job_title:
        role_score = 7
    elif 'solutions manager' in job_title and 'brand' in headline:
        role_score = 6  # Client-facing but not brand owner
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

    # Get columns dynamically
    cols = list(lead.keys())
    name = lead.get(cols[2], '') if len(cols) > 2 else ''
    company_name = lead.get(cols[8], '') if len(cols) > 8 else ''
    job_title = lead.get(cols[10], '') if len(cols) > 10 else ''

    in_golden_sheet = lead.get('brand_in_golden_sheet', 'No')
    total_assets = lead.get('total_assets_tested', '0')
    category_name = lead.get('company_category', '')
    category_assets = lead.get('category_asset_count', '0')

    # Determine seniority level
    if seniority >= 8.5:
        seniority_desc = "Senior/Director-level"
    elif seniority >= 7:
        seniority_desc = "Mid-Senior level"
    elif seniority >= 6:
        seniority_desc = "Manager-level"
    else:
        seniority_desc = "Junior/Entry-level"

    # Golden Sheet presence
    if in_golden_sheet == 'Yes':
        if int(total_assets) >= 50:
            golden_desc = f"IN Golden Sheet with {total_assets} assets (excellent signal)"
        elif int(total_assets) >= 20:
            golden_desc = f"IN Golden Sheet with {total_assets} assets (strong signal)"
        else:
            golden_desc = f"IN Golden Sheet ({total_assets} assets)"
    else:
        golden_desc = "NOT in Golden Sheet"

    # Category
    if category_name:
        category_desc = f"{category_name}"
    else:
        category_desc = "Unknown category"

    # Role assessment
    if role >= 9:
        role_desc = "Perfect brand marketing fit"
    elif role >= 7:
        role_desc = "Strong brand focus"
    elif role >= 5:
        role_desc = "Moderate brand involvement"
    else:
        role_desc = "Limited marketing authority"

    reasoning = f"{seniority_desc} at {company_name}. {golden_desc}. {category_desc}. {role_desc}."

    return reasoning[:300]

# Process all leads
with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    leads = list(reader)

# Add new columns (remove if already exists)
new_fieldnames = [f for f in fieldnames if f not in ['icp_score', 'score_reasoning']]
new_fieldnames.extend(['icp_score', 'score_reasoning'])

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
print(f"BATCH 5 SCORING SUMMARY")
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
    cols = list(lead.keys())
    name = lead.get(cols[2], '') if len(cols) > 2 else ''
    company = lead.get(cols[8], '') if len(cols) > 8 else ''
    job_title = lead.get(cols[10], '') if len(cols) > 10 else ''

    print(f"{i}. {name} - Score: {lead['icp_score']}")
    print(f"   Company: {company}")
    print(f"   Title: {job_title}")
    print(f"   Golden Sheet: {lead['brand_in_golden_sheet']} ({lead.get('total_assets_tested', 0)} assets)")
    print(f"   Reasoning: {lead['score_reasoning']}")
    print()

print(f"{'='*60}")
print(f"Output saved to: {output_file}")
print(f"{'='*60}\n")
