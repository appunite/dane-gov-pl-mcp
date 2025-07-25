import argparse
import asyncio

from src.utils.server_config import mcp
from src.tools import datasets, resources, institutions, showcases, tabular, parsers, utils # noqa: F401
from src.prompts import prompts # noqa: F401



async def _log_registered_tools():
    """Log the registered tools for debugging."""
    print("🔧 Registered MCP Tools:")
    try:
        tools = await mcp.get_tools()
        if tools:
            print(f"✅ Found {len(tools)} registered tools:")
            for i, tool in enumerate(tools, 1):
                if isinstance(tool, dict):
                    name = tool.get('name', 'Unknown')
                elif hasattr(tool, 'name'):
                    name = getattr(tool, 'name', 'Unknown')
                else:
                    name = str(tool)
                
                print(f"\t{i}. {name}")
            print()
            return
    except Exception as e:
        print(f"❌ Error getting tools: {e}\n")
    print(f"⚠️ Could not retrieve registered tools\n")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run the dane-gov-pl-mcp server.")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http", "sse"],
        default="stdio",
        help="Transport method for the server (default: stdio)."
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind the server to (default: 0.0.0.0)."
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind the server to (default: 8000)."
    )
    parser.add_argument(
        "--path",
        required=False,
        help="Mount path for HTTP/SSE (default /mcp or /sse)."
    )
    parser.add_argument(
        "--debug",
        choices=["True", "False"],
        default="False",
        help="Enable debug mode."
    )

    parser_args = parser.parse_args()

    if parser_args.transport == "sse" and parser_args.path is None:
        parser_args.path = parser_args.path or "/sse"
    elif parser_args.transport == "streamable-http" and parser_args.path is None:
        parser_args.path = parser_args.path or "/mcp"
    else:
        parser_args.path = None

    parser_args.debug = parser_args.debug != "False"

    return parser_args


if __name__ == "__main__":
    args = parse_args()

    if args.debug:
        asyncio.run(_log_registered_tools())
    
    if args.transport == "stdio":
        if args.debug:
            print("Starting MCP server in stdio mode...")
        mcp.run(
            transport=args.transport,
            show_banner=args.debug,
        )

    elif args.transport == "streamable-http":
        if args.debug:
            print(f"Starting MCP server in streamable-http mode at http://{args.host}:{args.port}{args.path}")
        mcp.run(
            transport=args.transport,
            path=args.path,
            host=args.host,
            port=args.port,
            show_banner=args.debug,
        )

    elif args.transport == "sse":
        if args.debug:
            print(f"Starting MCP server in SSE mode at http://{args.host}:{args.port}{args.path}")
        mcp.run(
            transport=args.transport,
            path=args.path,
            host=args.host,
            port=args.port,
            show_banner=args.debug,
        )

