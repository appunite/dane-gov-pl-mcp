import httpx
import io
import json
import re

from unstructured.partition.pdf import partition_pdf
from unstructured.partition.csv import partition_csv
from unstructured.partition.auto import partition
from unstructured.cleaners.core import clean_extra_whitespace, clean_bullets, clean_ordered_bullets

from src.utils.server_config import mcp, _TIMEOUT, AVAILABLE_FORMATS
from src.tools.utils import _get



@mcp.tool()
async def list_file_formats() -> list[str]:
    """
    Lists all supported file formats for document parsing.
    Returns information about what file types can be processed.
    """
    return AVAILABLE_FORMATS


@mcp.tool()
async def get_file_content(resource_ids: list[int]) -> dict:
    """
    Downloads file content for given resource IDs. `media_type` for the resource must be 'file'.
    Available file formats can be listed with `list_file_formats` tool.
    Maximum number of resources in one request is 100.
    """

    params = {
        "id[terms]": ",".join(str(id) for id in resource_ids),
        "per_page": 100,
    }
    resources = await _get(f"/resources", params=params)
    resources = resources.get("data", [])

    results = {}
    for resource in resources:
        resource_id = resource.get("id")
        url = resource.get("attributes").get("download_url")

        if resource.get("attributes").get("media_type") != "file":
            results[resource_id] = f"media_type {resource.get('attributes').get('media_type')} is not a 'file'"
            continue
        if resource.get("attributes").get("format", "").lower() not in AVAILABLE_FORMATS:
            results[resource_id] = f"format {resource.get('attributes').get('format')} is not supported"
            continue
        if not url:
            results[resource_id] = "download_url not found"
            continue

        results[resource_id] = await fetch_file_content(url, format=resource.get("attributes").get("format").lower())

    for resource_id in resource_ids:
        if str(resource_id) not in results:
            results[str(resource_id)] = "invalid resource_id"

    return results


async def fetch_file_content(url: str, format: str) -> str:
    """Fetch and parse file content using Unstructured for LLM-ready output."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=_TIMEOUT, follow_redirects=True)

            if response.status_code != 200:
                return f"HTTP {response.status_code}: Failed to fetch file"

            # Use Unstructured for document parsing
            if format == "pdf":
                return await parse_pdf_content(response.content)
            elif format in ["docx", "doc"]:
                return await parse_docx_content(response.content)
            elif format == "html":
                return await parse_html_content(response.text)
            elif format in ["xlsx", "xls"]:
                return await parse_excel_content(response.content)
            elif format in ["pptx", "ppt"]:
                return await parse_powerpoint_content(response.content)
            elif format == "csv":
                return await parse_csv_content(response.text)
            elif format == "json":
                return await parse_json_content(response.text)
            elif format == "txt":
                return clean_text_for_llm(response.text)
            else:
                return response.text

    except Exception as e:
        return f"Error fetching file: {str(e)}"


async def parse_json_content(json_text: str) -> str:
    """Parse JSON content."""
    try:
        parsed = json.loads(json_text)
        return json.dumps(parsed, indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        return json_text


def clean_text_for_llm(text: str) -> str:
    """Clean text for optimal LLM consumption."""

    # Remove excessive whitespace and clean structure
    text = clean_extra_whitespace(text)
    text = clean_bullets(text)
    text = clean_ordered_bullets(text)

    # Fix common issues
    text = re.sub(r'\n{3,}', '\n\n', text)  # Max 2 consecutive newlines
    text = re.sub(r'[ \t]{2,}', ' ', text)  # Multiple spaces to single space

    return text.strip()


def elements_to_markdown(elements) -> str:
    """Convert Unstructured elements to clean markdown format."""
    markdown_sections = []

    for element in elements:
        if not hasattr(element, 'text') or not element.text.strip():
            continue

        text = element.text.strip()

        # Format based on element type
        if element.category == "Title":
            markdown_sections.append(f"# {text}")
        elif element.category == "Header":
            markdown_sections.append(f"## {text}")
        elif element.category == "Table":
            # Preserve table structure
            markdown_sections.append(f"```\n{text}\n```")
        elif element.category == "ListItem":
            markdown_sections.append(f"- {text}")
        elif element.category == "NarrativeText":
            markdown_sections.append(text)
        else:
            markdown_sections.append(text)

    content = "\n\n".join(markdown_sections)
    return clean_text_for_llm(content)


async def parse_pdf_content(pdf_bytes: bytes) -> str:
    """Extract structured content from PDF using Unstructured."""
    try:
        elements = partition_pdf(file=io.BytesIO(pdf_bytes), strategy="polish")
        return elements_to_markdown(elements)
    except Exception as e:
        return f"Error parsing PDF: {str(e)}"


async def parse_docx_content(docx_bytes: bytes) -> str:
    """Extract structured content from DOCX using Unstructured."""
    try:
        elements = partition(file=io.BytesIO(docx_bytes), content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        return elements_to_markdown(elements)
    except Exception as e:
        return f"Error parsing DOCX: {str(e)}"


async def parse_html_content(html_text: str) -> str:
    """Extract structured content from HTML using Unstructured."""
    try:
        elements = partition(text=html_text, content_type="text/html")
        return elements_to_markdown(elements)
    except Exception as e:
        return f"Error parsing HTML: {str(e)}"


async def parse_excel_content(excel_bytes: bytes) -> str:
    """Extract structured content from Excel using Unstructured."""
    try:
        elements = partition(file=io.BytesIO(excel_bytes), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        return elements_to_markdown(elements)
    except Exception as e:
        return f"Error parsing Excel: {str(e)}"


async def parse_powerpoint_content(ppt_bytes: bytes) -> str:
    """Extract structured content from PowerPoint using Unstructured."""
    try:
        elements = partition(file=io.BytesIO(ppt_bytes), content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation")
        return elements_to_markdown(elements)
    except Exception as e:
        return f"Error parsing PowerPoint: {str(e)}"


async def parse_csv_content(csv_text: str) -> str:
    """Parse CSV content using Unstructured and format as clean markdown table."""
    try:
        elements = partition_csv(text=csv_text, encoding="utf-8")
        
        # Convert elements to clean markdown
        table_text = ""
        for element in elements:
            if hasattr(element, 'text') and element.text.strip():
                table_text += element.text + "\n"
        
        return clean_text_for_llm(table_text) if table_text else csv_text
        
    except Exception as e:
        # Final fallback: return as code block
        return f"```csv\n{csv_text}\n```"


# if __name__ == "__main__":
#     import asyncio
#     x = asyncio.run(get_file_content([8, 12, 40, 58]))
#     print(x)