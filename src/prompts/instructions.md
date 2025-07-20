# MCP Server dane.gov.pl - Usage Guidelines
When using the dane.gov.pl MCP server, follow these guidelines to find relevant Polish government datasets:

### Search Strategy
**Use progressive search refinement:**
1. Start not too broad but still with minimal filters, e.g., `query_all`, `keywords_terms`.
2. If too many results, add more filters, e.g., `category`, `title`, `institution`.
3. If still too broad, ask user to specify topic or date ranges.

**When you get zero results:**
1. Remove the most restrictive filters first
2. Remove `categories_2` first (the AND operator between categories_1 and categories_2 is very restrictive)
3. Try broader keywords instead of specific terms
4. Remove date constraints (`created_from`/`created_to`)
5. Check if you're mixing unrelated categories (e.g., Health + Environment will return nothing)

### Category Usage

**Understand the dual category system:**
- Always call `list_categories_1` and `list_categories_2` to see available options
- `categories_1` and `categories_2` are separate classification systems
- Using both creates an AND filter (very restrictive)
- Recommended: use only `categories_1` unless you need precise intersection
- Never mix unrelated categories from different systems

### Pagination and Results

**Handle large result sets:**
- Start with `per_page: 10` to get overview
- Use `page: 2, 3, etc.` to browse through results
- Sort by `views_count` (desc) to get most popular datasets first
- Sort by `created` (desc) to get newest datasets first

### Keyword Strategy
**Use multiple approaches:**
- `query_all`: searches everything with boolean operators
- `keywords_terms`: comma-separated exact keyword matching
- `title_match`: searches in titles only
- `title_phrase`: exact phrase matching in titles

**Polish content:**
- Most of the content is Polish, use Polish language for searching (`środowisko`, `zanieczyszczenie`, etc.)
- Use plural and singular forms

### Institution Filtering
**Find relevant institutions if user specifies interest:**
- Search institutions by city, description, or name
- Get institution IDs, then filter datasets by `institution_terms`
- Focus on major institutions like GUS (ID: 34), ministries, or research institutes

### Example Workflow
1. Call `ai-gov-pl:instructions`
2. List categories to understand classification
3. Start with broad search using `query_all` and `key_words`
4. Examine results and note relevant category IDs
5. Refine search with appropriate category filter
6. Get detailed information for promising datasets
7. If needed, explore the responsible institutions
8. Use pagination to browse through larger result sets

Remember: The server contains around 4,000 datasets and 10,000 resources, so start not too broad and progressively narrow down your search criteria.