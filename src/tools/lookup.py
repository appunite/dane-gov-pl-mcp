from async_lru import alru_cache

from src.utils.server_config import mcp
from src.tools.utils import _get



@alru_cache(ttl=3600)
async def _fetch_datasets() -> list[dict]:
    data = await _get("/datasets")
    return data.get("data", [])

@mcp.tool()
async def list_datasets() -> list[dict]:
    """Return all datasets (ID + title + description)."""
    return [
        {
            "id": x["id"], 
            "title": x["attributes"]["title"],
        } 
        for x in await _fetch_datasets()
    ]

@mcp.tool()
async def get_dataset_details(dataset_id: int) -> dict:
    """Return entire dataset data."""
    data = await _get(f"/datasets/{dataset_id}")
    return data.get("data", {})


# if __name__ == "__main__":
#     # import asyncio
#     # x = asyncio.run(list_datasets())
#     # print(f"{x}\n{len(x)}")
