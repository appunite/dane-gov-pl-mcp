from typing import Optional, List, Literal

from pydantic import BaseModel, Field, field_validator
import polars as pl

from src.utils.server_config import mcp
from src.tools.utils import _get

class DatasetSearchFilters(BaseModel):
    """Data object for dataset search.
    NOTE: there is AND operator between category filters, so setting both similar categories might reduce number of results. Using only one of them is recommended."""
    page: Optional[int] = Field(1, description="Page number, default 1")
    per_page: Optional[int] = Field(25, description="Number of items per page, default 25. Max is 100")
    
    query_all: Optional[str] = Field(None, description="Query string for everything with wildcards and boolean operators. (e.g., 'economics water')")
    
    sort: Optional[Literal["id", "title", "modified", "created", "views_count", "verified"]] = Field(None, description="Sort by field. Default order is ascending.")
    sort_order: Optional[Literal["asc", "desc"]] = Field(None, description="Sort order.")

    title_match: Optional[str] = Field(None, description="Searches for instances in which title matches the provided value. (e.g., 'economics')")
    title_prefix: Optional[str] = Field(None, description="Searches for instances that starts with the provided value in title. (e.g., 'eco')")
    title_phrase: Optional[str] = Field(None, description="searches for instances in which title exactly matches the provided phrase. (e.g., 'water level')")


    keywords_term: Optional[str] = Field(None, description="Filters instances in which keyword matches to the provided single value. (e.g., 'environment')")
    keywords_terms: Optional[str] = Field(None, description="Filters instances in which keyword matches to any of the provided values. (e.g., 'water', 'water,environment')")

    notes_match: Optional[str] = Field(None, description="Searches for instances in which notes matches the provided value. (e.g., 'economics')")
    
    categories_1: Optional[str] = Field(None, description="Filters instances in which categories from list_categories_1 ID matches to any of the provided value. (e.g., '140', '140,139')")
    categories_2: Optional[str] = Field(None, description="Filters instances in which categories from list_categories_2 ID matches to any of the provided value. (e.g., '43', '43,20')")

    institution_terms: Optional[str] = Field(None, description="Filters instances in which institution ID matches to any of the provided value. (e.g., '24', '24,123')")

    @field_validator("page")
    def validate_page(cls, v):
        if v < 1:
            raise ValueError("Page number must be greater than 0")
        return v
    
    @field_validator("per_page")
    def validate_per_page(cls, v):
        if v < 1 or v > 100:
            raise ValueError("Per page must be between 1 and 100")
        return v


@mcp.tool()
async def search_datasets_advanced(search_filters: DatasetSearchFilters) -> dict:
    """Advanced dataset search with filters."""
    params = {}
    if search_filters.page:
        params["page"] = search_filters.page
    if search_filters.per_page:
        params["per_page"] = search_filters.per_page

    if search_filters.query_all:
        params["q"] = search_filters.query_all

    if search_filters.sort:
        if search_filters.sort_order:
            if search_filters.sort_order == "asc":
                params["sort"] = search_filters.sort
            else:
                params["sort"] = f"-{search_filters.sort}"

    if search_filters.title_match:
        params["title[match]"] = search_filters.title_match
    if search_filters.title_prefix:
        params["title[prefix]"] = search_filters.title_prefix
    if search_filters.title_phrase:
        params["title[phrase]"] = search_filters.title_phrase
    
    if search_filters.keywords_term:
        params["keywords[term]"] = search_filters.keywords_term
    if search_filters.keywords_terms:
        params["keywords[terms]"] = search_filters.keywords_terms

    if search_filters.notes_match:
        params["notes[match]"] = search_filters.notes_match

    if search_filters.categories_1:
        params["categories[id][terms]"] = search_filters.categories_1
    if search_filters.categories_2:
        params["category[id][terms]"] = search_filters.categories_2

    if search_filters.institution_terms:
        params["institution[id][terms]"] = search_filters.institution_terms

    data = await _get("/datasets", params=params)
    return data.get("data", {})


@mcp.tool()
async def get_datasets_details(dataset_ids: list[int]) -> list[dict]:
    """Return entire dataset data."""
    details = []
    for id in dataset_ids:
        data = await _get(f"/datasets/{id}")
        details.append(data.get("data", {}))
    return details


@mcp.tool()
async def list_categories_1() -> dict:
    """List all available categories_1 for dataset filtering."""
    # Load ./data/categories_unique.csv
    try:
        df = pl.read_csv("./data/categories_unique.csv")
    except FileNotFoundError:
        raise FileNotFoundError("Category 1 file not found. Run `python -m src.utils.update_categories` to update the file.")
    return df.to_dicts()


@mcp.tool()
async def list_categories_2() -> dict:
    """List all available categories_2 for dataset filtering."""
    # Load ./data/category_unique.csv
    try:
        df = pl.read_csv("./data/category_unique.csv")
    except FileNotFoundError:
        raise FileNotFoundError("Category 2 file not found. Run `python -m src.utils.update_categories` to update the file.")
    return df.to_dicts()


# if __name__ == "__main__":
#     import asyncio
#     x = asyncio.run(list_categories_1())
#     print(f"{x}\n{len(x)}")
