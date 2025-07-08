from pathlib import Path

from src.utils.server_config import mcp



@mcp.tool()
def instructions() -> str:
    """Return the general guidelines for the whole MCP server. The guidliness must be loaded before using any other tools from this server."""
    path = Path(__file__).parent / "instructions.md"
    return path.read_text(encoding="utf-8")
