from typing import Optional, Literal

from pydantic import BaseModel, Field, field_validator

from src.utils.server_config import mcp
from src.tools.utils import _get



class TabularDataFilters(BaseModel):
    """Data object for tabular data filtering."""
    page: Optional[int] = Field(1, description="Page number, default 1")
    per_page: Optional[int] = Field(25, description="Number of items per page, default 25. Max is 100")
    
    q: Optional[str] = Field(None, description="Query string for filtering specific rows. Supports field-specific search (col1:value), wildcards (?, *), regex (/pattern/), fuzziness (~), proximity searches, ranges [min TO max], boolean operators (AND, OR), and grouping with parentheses. (e.g., 'col3:Nowak AND col1:Mazowieckie', 'kow*', '/kow[eai]lski/', 'nowk~')")
    
    sort: Optional[str] = Field(None, description="Sort by field. Default order is ascending. Must be in format 'colN' where N is a positive integer representing column number.")
    sort_order: Optional[Literal["asc", "desc"]] = Field(None, description="Sort order.")

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

    @field_validator("sort")
    def validate_sort(cls, v):
        v = v.lstrip("-")
        if v is None:
            return v
        if not v.startswith("col"):
            raise ValueError("Sort field must start with 'col'")
        try:
            col_num = int(v[3:])
            if col_num < 1:
                raise ValueError("Column number must be greater than 0")
        except ValueError:
            raise ValueError("Sort field must be in format 'colN' where N is a positive integer")
        return v

@mcp.tool()
async def get_tabular_resource_metadata(resource_ids: list[int]) -> dict:
    """Get tabular resources metadata including data schema, headers, aggregations, count of rows and first row."""
    params = {
        "per_page": 1,
    }
    result = {}
    for id in resource_ids:
        data = await _get(f"/resources/{id}/data", params=params)
        meta = data.get("meta", {})
        data = data.get("data", {})
        row_data = {}
        row_data["count"] = meta.get("count", 0)
        row_data["data_schema"] = meta.get("data_schema", {})
        row_data["headers_map"] = meta.get("headers_map", {})
        row_data["aggregations"] = meta.get("aggregations", {})
        row_data["rows"] = data[0].get("attributes", {})
        result[id] = row_data
    return result


@mcp.tool()
async def get_tabular_data(resource_id: int, search_filters: TabularDataFilters) -> dict:
    """Search and filter tabular data within a specific resource using advanced query capabilities. 
    Returns metadata androws as a list of dictionaries."""
    params = {}
    if search_filters.page:
        params["page"] = search_filters.page
    if search_filters.per_page:
        params["per_page"] = search_filters.per_page
    
    if search_filters.q:
        params["q"] = search_filters.q
    
    if search_filters.sort:
        if search_filters.sort_order == "desc":
            params["sort"] = f"-{search_filters.sort}"
        else:
            params["sort"] = search_filters.sort
    

    data = await _get(f"/resources/{resource_id}/data", params=params)
    
    result = {
        "data": [
            {k: v.get("val") for k, v in x.get("attributes", {}).items()}
            for x in data.get("data", [])
        ],
        "meta": {
            "count": data.get("meta", {}).get("count", 0),
            "params": data.get("meta", {}).get("params", {})
        }
    }
    
    return result


# if __name__ == "__main__":
#     import asyncio
#     x = asyncio.run(get_tabular_data(28052, TabularDataFilters(q="col1:Koz*", sort="-col2", sort_order="desc")))
#     print(f"{x}\n{len(x)}")
