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

The project is currently in the **exploration and validation phase**. Core discovery functionality is implemented and working:

### ✅ Available Features
- **Institution Search** - Find and filter government institutions by name, city, description
- **Dataset Discovery** - Search datasets by keywords, titles, and descriptions  
- **Resource Listing** - Browse individual data files within datasets
- **Metadata Access** - Get detailed information about institutions, datasets, and resources

### 🔄 In Development
- **Data Parsing Layer** - Convert resources into common Polars DataFrame format
- **LLM Processing** - Enable grouping, aggregating, filtering, and sorting operations
- **Chart Integration** - Visualize processed data through MCP chart tools

## 🏗️ Architecture

The system comprises three distinct functionality layers:

```
┌─────────────────┐
│    Discovery    │  ← Search & filter datasets/resources/institutions
├─────────────────┤
│     Parsing     │  ← Convert all resources to Polars DataFrames  
├─────────────────┤
│   Processing    │  ← LLM-powered operations on structured data
└─────────────────┘
```

### Discovery Layer
- Search datasets by keywords (e.g., "water condition")
- Filter institutions by city, name, or description
- Browse resources within selected datasets
- Access comprehensive metadata

### Parsing Layer *(Planned)*
- Convert CSV, JSON, XML, HTML and other files to Polars DataFrames
- Support for most of the resources in optimal formats

### Processing Layer *(Planned)*  
- Data operations (group, filter, aggregate)
- Integration with visualization tools

## 🛠️ Tech Stack

- **Python** - Core development language
- **FastMCP** - MCP server framework
- **Polars** - High-performance data processing
- **Pydantic** - Data validation and serialization
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

### Search for Environmental Data
```python
# Find all datasets related to air quality
search_datasets(query="air quality", category="environment")

# Get institutions monitoring water resources  
search_institutions(description_phrase="water", city_terms="Warszawa")
```

### Discover Government Resources
```python
# Browse all datasets from a specific institution
get_institution_datasets(institution_id=123)

# Get detailed metadata for a dataset
get_dataset_details(dataset_id=456)
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
