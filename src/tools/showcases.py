from typing import Optional, Literal

from pydantic import BaseModel, Field, field_validator

from src.utils.server_config import mcp
from src.tools.utils import _get



class ShowcaseSearchFilters(BaseModel):
    """Data object for showcase search."""
    page: Optional[int] = Field(1, description="Page number, default 1")
    per_page: Optional[int] = Field(25, description="Number of items per page, default 25. Max is 100")
    
    query_all: Optional[str] = Field(None, description="Query string for everything with wildcards and boolean operators. (e.g., 'data visualization')")
    
    sort: Optional[Literal["title", "date", "views_count"]] = Field(None, description="Sort by field. Default order is ascending.")
    sort_order: Optional[Literal["asc", "desc"]] = Field(None, description="Sort order.")

    institution_terms: Optional[str] = Field(None, description="Filters showcases in which institution ID matches to any of the provided value. (e.g., '24', '24,123')")

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
async def search_showcases(search_filters: ShowcaseSearchFilters) -> list[dict]:
    """Advanced showcase search with filters."""
    params = {"model": "showcase"}

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
        else:
            params["sort"] = search_filters.sort

    if search_filters.institution_terms:
        params["institution[terms]"] = search_filters.institution_terms

    params["model"] = "showcase"

    data = await _get("/search", params=params)
    data = data.get("data", [])
    return [
        {
            "id": x.get("id"),
            "title": x.get("attributes", {}).get("title"),
            "keywords": x.get("attributes", {}).get("keywords"),
            "showcase_category_name": x.get("attributes", {}).get("showcase_category_name"),
            "notes": x.get("attributes", {}).get("notes"),
            "author": x.get("attributes", {}).get("author"),
        }
        for x in data
    ]


@mcp.tool()
async def get_showcases_details(showcase_ids: list[int]) -> list[dict]:
    """Get details of specific showcases."""
    details = []
    for id in showcase_ids:
        data = await _get(f"/showcases/{id}")
        data = data.get("data", {})
        details.append(data)
    return details


# if __name__ == "__main__":
#     import asyncio
#     search_filters = ShowcaseSearchFilters(
#         query_all="matura",
#     )
#     x = asyncio.run(search_showcases(search_filters))
#     x = asyncio.run(get_showcases_details([1310, 1288]))
#     print(x)



