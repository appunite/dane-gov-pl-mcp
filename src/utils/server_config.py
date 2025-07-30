from fastmcp import FastMCP
import httpx


_INSTRUCTIONS = """MCP server for dane.gov.pl API"""

AVAILABLE_FORMATS = ["csv", "tsv", "json", "geojson", "jsonld", "pdf", "docx", "doc", "html", "txt", "xlsx", "xls", "xml"]
TABULAR_FORMATS = ["csv", "tsv", "json", "xlsx", "xls"]

_TIMEOUT = httpx.Timeout(30.0, connect=5.0)
_API = f"https://api.dane.gov.pl/"

mcp = FastMCP(
    name="dane-gov-pl-mcp",
    instructions=_INSTRUCTIONS,
)