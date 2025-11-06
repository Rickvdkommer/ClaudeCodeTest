
# ENRICHMENT AGENT INSTRUCTIONS

## Your Task
Process your assigned batch of leads and enrich each row with Golden Sheet data.

## Input Files
- Your batch: agent_batches/enrichment_batch_[YOUR_NUMBER].json
- Category data: Golden Sheet - Category_Count.csv
- Pivot table: Golden Sheet - Pivot Table Brands.csv

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
