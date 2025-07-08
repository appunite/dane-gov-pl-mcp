from fastmcp import FastMCP



_INSTRUCTIONS = """MCP server for dane.gov.pl API"""


mcp = FastMCP(
    name="dane-gov-pl-mcp",
    instructions=_INSTRUCTIONS,
)