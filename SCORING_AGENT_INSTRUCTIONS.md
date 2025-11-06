
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
