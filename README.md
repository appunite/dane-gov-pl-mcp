
Cursor mcp.json config
``` json
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