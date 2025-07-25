from typing import Optional, Literal
from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from src.utils.server_config import mcp
from src.tools.utils import _get



class ResourceSearchFilters(BaseModel):
    """Data object for resource search."""
    page: Optional[int] = Field(1, description="Page number, default 1")
    per_page: Optional[int] = Field(25, description="Number of items per page, default 25. Max is 100")
    
    query_all: Optional[str] = Field(None, description="Query string for everything with wildcards and boolean operators. (e.g., 'economics water')")
    
    sort: Optional[Literal["id", "title", "modified", "created", "views_count", "verified"]] = Field(None, description="Sort by field. Default order is ascending.")
    sort_order: Optional[Literal["asc", "desc"]] = Field(None, description="Sort order.")

    title_match: Optional[str] = Field(None, description="Searches for instances in which title matches the provided value. (e.g., 'economics')")
    title_prefix: Optional[str] = Field(None, description="Searches for instances that starts with the provided value in title. (e.g., 'eco')")
    title_phrase: Optional[str] = Field(None, description="searches for instances in which title exactly matches the provided phrase. (e.g., 'water level')")

    created_from: Optional[str] = Field(None, description="Filters resources in which created_at is greater than or equal to the provided value. (e.g., '2024-03-20', '2021-03')")
    created_to: Optional[str] = Field(None, description="Filters resources in which created_at is less than or equal to the provided value. (e.g., '2024-03-20', '2021-03')")

    description_prefix: Optional[str] = Field(None, description="Searches for resources that starts with the provided value in description. (e.g., 'Nat')")
    description_phrase: Optional[str] = Field(None, description="searches for resources in which description matches the provided phrase. (e.g., 'National')")

    dataset_id_terms: Optional[str] = Field(None, description="Filters resources in which dataset ID matches to any of the provided value. (e.g., '2,5,7')")
    dataset_title_phrase: Optional[str] = Field(None, description="searches for resources in which dataset title matches the provided phrase. (e.g., 'water level')")
    
    id_terms: Optional[str] = Field(None, description="Filters resources in which ID matches to any of the provided value. (e.g., '2,5,7')")

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
    
    @field_validator("created_from", "created_to")
    def validate_created_date(cls, v):
        if v is None:
            return v
        formats = [
            "%Y-%m-%d",          # 2024-03-20
            "%Y-%m",             # 2024-03
            "%Y",                # 2024
            "%Y-%m-%dT%H:%M:%SZ" # 2017-03-29T09:26:22Z
        ]
        for fmt in formats:
            try:
                datetime.strptime(v, fmt)
                return v
            except ValueError:
                continue
        raise ValueError("Date must be in format YYYY-MM-DD, YYYY-MM, YYYY or YYYY-MM-DDThh:mm:ssZ")


@mcp.tool()
async def search_resources(search_filters: ResourceSearchFilters) -> list[dict]:
    """Advanced resource search with filters."""
    params = {}
    if search_filters.page:
        params["page"] = search_filters.page
    if search_filters.per_page:
        params["per_page"] = search_filters.per_page

    if search_filters.query_all:
        params["q"] = search_filters.query_all

    if search_filters.sort:
        if search_filters.sort_order == "desc":
            params["sort"] = f"-{search_filters.sort}"
        else:
            params["sort"] = search_filters.sort

    if search_filters.title_match:
        params["title[match]"] = search_filters.title_match
    if search_filters.title_prefix:
        params["title[prefix]"] = search_filters.title_prefix
    if search_filters.title_phrase:
        params["title[phrase]"] = search_filters.title_phrase
    
    if search_filters.created_from:
        params["created[gte]"] = search_filters.created_from
    if search_filters.created_to:
        params["created[lte]"] = search_filters.created_to

    if search_filters.description_prefix:
        params["description[prefix]"] = search_filters.description_prefix
    if search_filters.description_phrase:
        params["description[phrase]"] = search_filters.description_phrase
    
    if search_filters.dataset_id_terms:
        params["dataset[id][terms]"] = search_filters.dataset_id_terms
    if search_filters.dataset_title_phrase:
        params["dataset[title][phrase]"] = search_filters.dataset_title_phrase

    data = await _get(f"/resources", params=params)
    data = data.get("data", [])
    return [
        {
            "id": x.get("id"),
            "title": x.get("attributes", {}).get("title"),
            "description": x.get("attributes", {}).get("description"),
            "format": x.get("attributes", {}).get("format"),
            "file_size": x.get("attributes", {}).get("file_size"),
            "download_url": x.get("attributes", {}).get("download_url"),
            "created": x.get("attributes", {}).get("created"),
            "data_date": x.get("attributes", {}).get("data_date"),
            "dataset_id": x.get("relationships", {}).get("dataset", {}).get("data", {}).get("id"),
            "institution_id": x.get("relationships", {}).get("institution", {}).get("data", {}).get("id"),
            "media_type": x.get("attributes", {}).get("media_type"),
        }
        for x in data
    ]


@mcp.tool()
async def get_resources_details(resource_ids: list[int]) -> list[dict]:
    """Get details of a specific resources."""
    details = []
    for id in resource_ids:
        data = await _get(f"/resources/{id}")
        data = data.get("data", {})
        attributes = data.get("attributes", {})
        attributes["id"] = data.get("id")
        attributes["dataset_id"] = data.get("relationships", {}).get("dataset", {}).get("data", {}).get("id", None)
        attributes["institution_id"] = data.get("relationships", {}).get("institution", {}).get("data", {}).get("id", None)
        details.append(attributes)
    return details


# if __name__ == "__main__":
#     import asyncio
#     x = asyncio.run(get_resources_details([123]))
#     print(f"{x}\n{len(x)}")
