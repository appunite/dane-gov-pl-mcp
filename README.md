# Dane.gov.pl MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

A **Model Context Protocol (MCP) Server** that integrates with [dane.gov.pl](https://dane.gov.pl), Poland's central open data portal. This server acts as a bridge between Polish public datasets and modern AI applications, creating a transparent, fast, and structured API layer consumable by LLMs, agents, and intelligent services.

Inspired by the success of [data-gov-il-mcp](https://github.com/DavidOsherProceed/data-gov-il-mcp), this project aims to unlock the potential of Polish government data for AI-powered civic applications.

## 🎯 Project Vision

**We are building the first MCP server for Polish open government data.** No equivalent exists today, making this a strategic opportunity to position Poland at the forefront of civic tech and AI infrastructure.

### The Problem
- Poland's <dane.gov.pl> has rich datasets but **poor accessibility for AI models**
- Inconsistent data formats and lack of clear APIs
- No standardized way for LLMs to access Polish government data

### The Solution
An open-source MCP Server that:
- 🔍 **Discovers** datasets through semantic search and filtering
- 🔄 **Parses** diverse data formats into unified structures  
- 🧠 **Processes** data with LLM-powered operations
- 📊 **Visualizes** results through chart integrations
- ⚡ **Aggregates** large datasets using Polars for performance

## 🚀 Current State

The project is in its **final stages of development**. All core functionality is implemented and tested, with ongoing quality-of-life improvements and bug fixes being made:

### ✅ Available Features
- **Institution Search** - Find and filter government institutions by name, city, description
- **Dataset Discovery** - Search datasets by keywords, titles, and descriptions  
- **Resource Listing** - Browse individual data files within datasets
- **Showcases Search** - Find real-world visualizations and applications that use the datasets
- **Metadata Access** - Get detailed information about institutions, datasets, and resources
- **Data Parsing** - Convert resources into LLM-ready Markdown documents
- **LLM Processing** - Enable grouping, aggregating, filtering, and sorting operations for tabular data resources (loaded into Polars DataFrames)
- **Chart Integration** - Visualize processed data through MCP chart tools

## 🏗️ Architecture

The system comprises three distinct functionality layers:

```
┌─────────────────┐
│    Discovery    │  ← Search & filter datasets/resources/institutions/showcases
├─────────────────┤
│     Parsing     │  ← Convert resources LLM ready Markdown documents 
├─────────────────┤
│   Processing    │  ← LLM-powered operations on Polars dataframes
└─────────────────┘
```

### Discovery Layer
- Search datasets by keywords (e.g., "water condition")
- Filter institutions by city, name, or description
- Browse resources within selected datasets
- Access comprehensive metadata

### Parsing Layer
- Convert CSV, JSON, XLSX, PDF and other files to Markdown documents
- Support for most of the resources in optimal formats

### Processing Layer
- Data operations (group, filter, aggregate)
- Integration with visualization tools

## 🛠️ Tech Stack

- **Python** - Core development language
- **FastMCP** - MCP server framework
- **Pydantic** - Data validation and serialization
- **Polars** - High-performance data processing
- **Unstructured** - Document parsing and extraction for PDFs, CSVs and other formats
- **Fly.io** - Deployment platform

## 🚀 Installation & Setup

### Prerequisites
- Python 3.11+
- UV package manager (recommended)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/appunite/dane-gov-pl-mcp.git
cd dane-gov-pl-mcp

# Install dependencies
uv sync

# Run the MCP server
uv run python -m src.app --transport stdio
```

### MCP Client Configuration

Add to your MCP client configuration (e.g., Claude Desktop, Cursor):

```json
{
  "mcpServers": {
    "dane-gov-pl-mcp": {
      "command": "/path/to/dane-gov-pl-mcp/.venv/bin/python",
      "args": ["-m", "src.app", "--transport", "stdio"],
      "cwd": "/path/to/dane-gov-pl-mcp",
      "env": {
        "PYTHONPATH": "/path/to/dane-gov-pl-mcp",
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

## 📖 Usage Examples

### Discovery
```python
# Search datasets by keywords
search_datasets(search_filters={"query_all": "environment"})

# Find institutions by location
search_institutions(search_filters={"city_terms": "Warszawa"})

# Get dataset details
get_resources_details(dataset_ids=[123, 456])
```

### Document Parsing
```python
# Parse files to Markdown
get_file_content(resource_ids=[123, 456])
```

### Tabular Data Processing
```python
# Get resource metadata
get_tabular_resource_metadata(resource_ids=[123])

# Query tabular data
get_tabular_data(resource_id=123, search_filters={"q": "col1:Warszawa"})

# Advanced DataFrame operations
resource_to_dataframe(resource_id=123, dataframe_operations={
    "primary_group": "col1",
    "aggregations": ["sum", "mean"],
    "aggregation_columns": ["col2", "col2"],
    "sort_columns"=["col2_sum", "col1"], 
    "sort_descending"=[True, False]
})
```

## 🤝 Contributing

We welcome contributions! This project aims to make Polish government data more accessible and usable for everyone.

### Contributing Guidelines
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 🔗 Links
- [dane.gov.pl](https://dane.gov.pl) - Poland's Open Data Portal
- [API Documentation](https://api.dane.gov.pl/doc) - Official API docs
- [Technical Standard](https://dane.gov.pl/media/ckeditor/2020/06/16/standard-techniczny.pdf) - Data standards

## Authors

Dane-gov-pl-mcp is created by _Appunite_.

Since 2010, [Appunite](https://appunite.com/) is a collective of software engineers, product builders, and problem solvers. We partner with bold teams to tackle product and business challenges—designing custom-built squads that ship fast and think strategically. From AI-enabled workflows to mobile platforms and cloud-native systems, we deliver end-to-end execution with full accountability.

Looking for your unfair advantage in tech execution? [Talk to us](https://www.appunite.com/get-in-touch).

Copyright 2025, [Appunite](https://appunite.com/)

![Appunite](https://appunite-logo.s3.eu-central-1.amazonaws.com/Appunite-Logo-Long-Black-200.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAYXIIMCOVBPWI5LPJ%2F20250725%2Feu-central-1%2Fs3%2Faws4_request&X-Amz-Date=20250725T112818Z&X-Amz-Expires=300&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEBwaDGV1LWNlbnRyYWwtMSJGMEQCIHfIdYSUQEu%2FlT1lF%2FA7Fex2lKRykD7mJywiqlTFi%2FcUAiA9FuvTBLT%2FM8I5aaS%2F%2F2rpoHLegtwinRtCRVkdvV52wCraAghFEAAaDDU5OTcwMjM3NzM4NiIMTWQuYjXDhP%2BN0dTbKrcCAar%2FXqFN1E0Enf6%2FJ0svIQrwSaYFfdorLg1G2O1F%2BgihAjoIlenp%2FQsv%2FEkaPve4TVm2Hs2tPChPSx5Zd0Ukivg6%2FIrglZxgss4BbaOqzhXHKEDc9QOf%2BdBDAJm7T0syQrQRtfVArJbs9gzSOhjo3EM06ALAlicZRtYThKxct7vAnLqrP%2Bg98xwvNAKwNtweiu34yD%2Ba8S9HiY%2FslkWgYIvmYB91gWQko6wVSS5toiQlHjp%2FOfA9MztepfxYrSkBkRDO3wX6rf5RhUbvIpCxEkR%2BnVgXwPISVObyqBCdujb7iY8MoGdym7JqW%2BBIbhKr8tkbaxfbE0BpZXYvImlV7ENcWY2Jao8zILrdX3eXCx6VPRGv9RGh%2Bm7DZ36s7PCmAcCRnwxTVYBXYkTZ5q%2B5mFvg7emsGGgw3s6NxAY6rgJRJ3lIN%2BaxfzUEiQEhnYHxdAKH9PE2UN9fqZJYCg6%2BzlkymXeGW2povLck%2B%2FfbPldDNym60%2FnhGu028KLD0RMB6U52jRrcvhQXrmqvwpY4K0rG2O6OToowy6Ykgxk6aIocryj2QStaLIS92lydr1a3G0P7BFUZoXXI9%2BgfaDYRhQuAS7o74CxqEqXfvsNMcvZAqKck0MLqjw9qTZCoF%2Bj8TmOIl%2F1j7gWq5SNZ9dQ4cRrrmRKDvbUTtWFDyr6m6JPOL6z134oAZRzc4rw6IXtsEqlPvKI62gkjQjHbNN817nHjdwcgfCIRtCU6OqWFov5KPFg4JswdkzECUIlyZwXY0Va7V7zRjoUUvysQY3zbWn305nZNN%2BxJtN8rk3Gb4GblXAZgHwjkk7jZWskOSg%3D%3D&X-Amz-Signature=8dfc7eefb30311ae9afd52f4a4c1b44817e090fe7a4f90ceec273deef4bb2435&X-Amz-SignedHeaders=host&response-content-disposition=inline)

Licensed under the [MIT License](LICENSE)
