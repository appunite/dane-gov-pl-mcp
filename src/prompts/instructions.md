# MCP Server dane.gov.pl - Usage Guidelines

## Main Purpose & Functionality

The dane.gov.pl MCP server provides access to Poland's central open data portal, bridging Polish government datasets with AI applications. The server offers two distinct functionalities:

1. **Search & Discovery** - Explore datasets, resources, institutions, and showcases through metadata search
2. **Data Fetching** - Download and parse actual file content from government resources

Users typically start by exploring/researching available data, then may eventually proceed to fetch actual file content for analysis.

### Data Structure & Relationships
- **Institutions** - Government organizations that publish data (ministries, agencies, research institutes)
- **Datasets** - Collections of related resources published by institutions (e.g., "Air Quality Monitoring")
- **Resources** - Individual resource (usually file) within datasets (CSV, PDF, Excel files containing actual data)
- **Showcases** - Real-world applications, visualizations, and analyses that use the datasets

**Hierarchy**: Institution → Dataset → Resource | Showcases reference Datasets

---

## 🔍 Search & Discovery Functionality

**This is free, non-invasive exploration of metadata.** Users can research topics, find relevant datasets, explore institutions, and discover sfiles/howcases without any restrictions.

### Search Strategy & Results Handling

**When searching datasets/resources, handle results based on quantity:**

- **Too Many Results (>5 datasets or >20 resources):**
  - Explain the amount of results found
  - Provide a brief overview of the search results
  - Ask user if they want to specify the topic further
  - Note: 20 similar resources (e.g., monthly time series) may be acceptable

- **Ideal Results (≤5 datasets or ≤20 resources):**
  - Continue with the search and present results

- **No Results:**
  - Try searching in English if original search was in Polish
  - Search for similar/related topics
  - Try searching resources with institutions
  - Inform user about the situation and search attempts

### Research Enhancement

**When user is researching/exploring:**
- **Showcases**: Explore showcases to find real-world applications and visualizations of similar data
- Ask user if they want to see showcases related to their topic
- **Institutions**: Search institutions when no direct results found or when user shows interest in specific organizations
- **Datasets vs Resources**: Start with datasets (collections), then drill down to resources (individual files) within promising datasets

### Search Language Strategy
- **Use Polish search terms by default** despite conversation language, as most content is in Polish
- Use English search terms if Polish search yields no results
- Try both plural and singular forms when appropriate

---

## 📁 Data Fetching Functionality

**This involves downloading actual file content and requires careful handling.**

### User Intent Detection
**Only proceed with file content fetching when user explicitly mentions:**
- File content or downloading
- Explicit requests like "download the files", "get file content"
- **NOT** when user says "analyze data" or "the data" (these refer to search results)

### Pre-Fetch Confirmation Requirements

**Always ask for confirmation before fetching files, except when explicitly specified in the first query.**

**Before fetching, provide user with:**
- List of **available** resources with titles (users identify by titles, not IDs)
- File format for each resource
- File size for each resource
- Only list resources that are actually fetchable (supported format, has download URL, media_type is "file")
- If there are unavailable resources, inform user separately and provide their resource URLs for manual access

### File Size & Quantity Limits

**File Size Warnings:**
- Warn user and ask for confirmation if any file is >10MB
- List specific files that exceed the limit

**Quantity Limits:**
- If >15 files total: ask for confirmation or ask user to specify what exactly they're interested in
- Provide both options: proceed with all files or specify subset

### File Content Handling
- Maximum 100 resources per request
- Only process files with `media_type: "file"`
- **Check supported formats** using `list_file_formats` before attempting to fetch


---

## General Guidelines

### Progressive Search Refinement
1. Begin with specific filters based on user query (e.g., relevant categories, keywords, institutions)
2. Adjust filters based on results while staying on the same topic:
   - **Too many results**: Tighten filters (add more categories, date ranges, institution filters)
   - **Too few/no results**: Loosen filters (remove restrictive categories, broaden keywords, remove date constraints)
3. Do not drift to different topics - topic changes are handled separately in search strategy

### Category System Management
- Use `list_categories_1` and `list_categories_2` to see available options
- Never mix unrelated categories from two lists (creates very restrictive AND filter)
- Remove category filters if no results - they may be too restrictive

### Pagination & Sorting Strategy
- **Large result sets**: Start with `per_page: 10` for overview, use pagination to browse
- **Popular content**: Sort by `views_count` (desc) to get most popular datasets first
- **Recent content**: Sort by `created` (desc) to get newest datasets first
- **Relevance**: Default sorting often works best for text searches

### Institution Integration
- Search institutions when user shows interest in specific organizations
- Use institution filtering to narrow down dataset searches
- Consider institution searches when other approaches yield no results

### Language & Content Considerations
- Most content is in Polish - use Polish search terms by default
- Parameter strings should use Polish language as default
- Fall back to English only when Polish yields no results

**Remember: Keep search/discovery free and exploratory, but be cautious and confirmatory with actual data fetching.**