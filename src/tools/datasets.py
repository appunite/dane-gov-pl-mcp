from async_lru import alru_cache
from pydantic import BaseModel

from src.utils.server_config import mcp
from src.tools.utils import _get

from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime



class DatasetSearchFilters(BaseModel):
    """Data object for dataset search.
    NOTE: there is AND operator between catefories_terms and category_terms filters, so setting both might reduce number of results.
    """

    page: Optional[int] = Field(None, description="Page number, default 1")
    per_page: Optional[int] = Field(None, description="Number of items per page, default 25")
    
    query_all: Optional[str] = Field(None, description="Query string for everything with wildcards and boolean operators. (e.g., '*rem* AND ipsum', 'lorem', 'lorem OR ipsum*')")
    
    sort: Optional[Literal["id", "title", "modified", "created", "views_count", "verified"]] = Field(None, description="Sort by field. Default order is ascending.")
    sort_order: Optional[Literal["asc", "desc"]] = Field(None, description="Sort order.")

    title_query: Optional[str] = Field(None, description="Searches for instances that matches the provided query string in title. (e.g., '*logi* AND water', 'economics', 'level OR eco*')")
    title_prefix: Optional[str] = Field(None, description="Searches for instances that starts with the provided value in title. (e.g., 'eco')")
    title_phrase: Optional[str] = Field(None, description="searches for instances in which title matches the provided phrase. (e.g., 'water level')")

    keywords_terms: Optional[str] = Field(None, description="Filters instances in which keyword matches to any of the provided value. (e.g., 'environment,economy')")
    keywords_contains: Optional[str] = Field(None, description="Filters instances in which keyword contains the provided value. (e.g., 'eco')")

    notes_match: Optional[str] = Field(None, description="Searches for instances in which notes matches the provided value. (e.g., 'economics')")
    notes_query: Optional[str] = Field(None, description="Searches for instances that matches the provided query string. (e.g., '*eco* AND water', 'lorem', 'lorem OR ipsum*')")
    
    categories_terms: Optional[str] = Field(None, description="Filters instances in which categories_1 ID matches to any of the provided value. (e.g., '140,139')")
    category_terms: Optional[str] = Field(None, description="Filters instances in which categories_2 ID matches to any of the provided value. (e.g., '140,139')")

    institution_terms: Optional[str] = Field(None, description="Filters instances in which institution ID matches to any of the provided value. (e.g., '24,123')")

# Example usage showing how nested models work
# @mcp.tool()
async def search_datasets_advanced(filters: DatasetSearchFilters) -> dict:
    """Advanced dataset search with filters."""
    #TODO apply filters to params
    params = {}
    data = await _get("/datasets", params=params)
    return data.get("data", {})


@alru_cache(ttl=3600)
async def _fetch_datasets() -> list[dict]:
    data = await _get("/datasets")
    return data.get("data", [])


@mcp.tool()
async def get_dataset_details(dataset_id: int) -> dict:
    """Return entire dataset data."""
    data = await _get(f"/datasets/{dataset_id}")
    return data.get("data", {})


@mcp.tool()
async def list_categories() -> dict:
    """List all available categories for dataset filtering."""
    pass

@mcp.tool()
async def list_category() -> dict:
    """List all available categories for dataset filtering."""
    pass


if __name__ == "__main__":
    import asyncio
    title_filter = TitleFilter()
    x = asyncio.run(search_datasets_advanced(title_filter, None, None, None))
    # print(f"{x}\n{len(x)}")
