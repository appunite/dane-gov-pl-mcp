from typing import Optional, Literal

from pydantic import BaseModel, Field, field_validator

from src.utils.server_config import mcp
from src.tools.utils import _get



class InstitutionSearchFilters(BaseModel):
    """Data object for institution search."""
    page: Optional[int] = Field(1, description="Page number, default 1")
    per_page: Optional[int] = Field(25, description="Number of items per page, default 25. Max is 100")
    
    query_all: Optional[str] = Field(None, description="Query string for everything with wildcards and boolean operators. (e.g., 'Nationnal')")
    
    sort: Optional[Literal["id", "title", "city", "modified", "created"]] = Field(None, description="Sort by field. Default order is ascending.")
    sort_order: Optional[Literal["asc", "desc"]] = Field(None, description="Sort order.")

    title_prefix: Optional[str] = Field(None, description="Searches for institutions that starts with the provided value in title. (e.g., 'eco')")
    title_phrase: Optional[str] = Field(None, description="searches for institutions in which title matches the provided phrase. (e.g., 'sport center')")

    description_prefix: Optional[str] = Field(None, description="Searches for institutions that starts with the provided value in description. (e.g., 'Nat')")
    description_phrase: Optional[str] = Field(None, description="searches for institutions in which description matches the provided phrase. (e.g., 'National')")

    city_terms: Optional[str] = Field(None, description="Filters institutions in which city matches to any of the provided value. (e.g., 'Warszawa,Poznan')")

    postal_code_terms: Optional[str] = Field(None, description="Filters institutions in which polish postal code matches to any of the provided value. (e.g., '00-000,00-001')")
    postal_code_startswith: Optional[str] = Field(None, description="Filters institutions in which polish postal code starts with the provided value. (e.g., '00-')")
    postal_code_endswith: Optional[str] = Field(None, description="Filters institutions in which polish postal code ends with the provided value. (e.g., '-001')")
    
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
async def search_institutions(search_filters: InstitutionSearchFilters) -> list[dict]:
    """List all available institutions for dataset filtering."""
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
        else:
            params["sort"] = search_filters.sort

    if search_filters.title_prefix:
        params["title[prefix]"] = search_filters.title_prefix
    if search_filters.title_phrase:
        params["title[phrase]"] = search_filters.title_phrase

    if search_filters.description_prefix:
        params["description[prefix]"] = search_filters.description_prefix
    if search_filters.description_phrase:
        params["description[phrase]"] = search_filters.description_phrase

    if search_filters.city_terms:
        params["city[terms]"] = search_filters.city_terms

    # NOTE: The api is incorrect, endswith means startswith, startswith means endswith. Keep this bug in mind, it might be fixed.
    if search_filters.postal_code_terms:
        params["postal_code[terms]"] = search_filters.postal_code_terms
    if search_filters.postal_code_startswith:
        params["postal_code[endswith]"] = search_filters.postal_code_startswith
    if search_filters.postal_code_endswith:
        params["postal_code[startswith]"] = search_filters.postal_code_endswith

    data = await _get("/institutions", params=params)
    data = data.get("data", [])
    return [
        {
            "id": x.get("id"),
            "title": x.get("attributes", {}).get("title"),
            "datasets_count": x.get("relationships", {}).get("datasets", {}).get("meta", {}).get("count", 0),
            "resources_count": x.get("relationships", {}).get("resources", {}).get("meta", {}).get("count", 0)
        }
        for x in data
    ]


@mcp.tool()
async def get_institutions_details(institution_ids: list[int]) -> list[dict]:
    """Get details of a specific institutions."""
    details = []
    for id in institution_ids:
        data = await _get(f"/institutions/{id}")
        data = data.get("data", {})
        attributes = data.get("attributes", {})
        attributes["id"] = data.get("id")
        attributes["datasets_count"] = data.get("relationships", {}).get("datasets", {}).get("meta", {}).get("count", 0)
        attributes["resources_count"] = data.get("relationships", {}).get("resources", {}).get("meta", {}).get("count", 0)
        details.append(attributes)
    return details


@mcp.tool()
async def get_institutions_datasets(institution_ids: list[int]) -> dict[int, list[dict]]:
    """Get datasets of a specific institutions."""
    # TODO: It also needs pagination and allows filtering etc.
    datasets = {}
    for id in institution_ids:
        data = await _get(f"/institutions/{id}/datasets")
        data = data.get("data", {})
        datasets[id] = [
            {
                "id": x.get("id"),
                "title": x.get("attributes", {}).get("title"),
                "resources_count": x.get("relationships", {}).get("resources", {}).get("meta", {}).get("count", 0),
                "showcases_count": x.get("relationships", {}).get("showcases", {}).get("meta", {}).get("count", 0)
            }
            for x in data
        ]
    return datasets



# if __name__ == "__main__":
    # import asyncio
    # x = asyncio.run(list_institutions())
    # institution = x[0]
    # print(institution)
    # x = asyncio.run(get_institution_details(institution.get("id")))
    # print(x)
    # x = asyncio.run(get_institution_datasets(institution.get("id")))
    # print(x)


