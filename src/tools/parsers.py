import httpx
import io
import json
import re
import csv

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
        url = resource.get("attributes").get("file_url")

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

            if format == "pdf":
                return await parse_pdf_content(response.content)
            elif format in ["docx", "doc"]:
                return await parse_docx_content(response.content)
            elif format == "html":
                return await parse_html_content(response.text)
            elif format in ["xlsx", "xls"]:
                return await parse_excel_content(response.content)
            elif format in ["csv", "tsv"]:
                return await parse_csv_content(response.text)
            elif format in ["json", "geojson", "jsonld"]:
                return await parse_json_content(response.text)
            elif format == "xml":
                return await parse_xml_content(response.text)
            elif format in ["txt"]:
                return clean_text_for_llm(response.text)
            else:
                return response.text

    except Exception as e:
        return f"Error fetching file: {str(e)}"


def clean_text_for_llm(text: str) -> str:
    """Clean text for optimal LLM consumption."""
    try:
        text = clean_extra_whitespace(text)
    except:
        pass
    try:
        text = clean_bullets(text)
    except:
        pass
    try:
        text = clean_ordered_bullets(text)
    except:
        pass

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

        if element.category == "Title":
            markdown_sections.append(f"# {text}")
        elif element.category == "Header":
            markdown_sections.append(f"## {text}")
        elif element.category == "Table":
            markdown_sections.append(f"```\n{text}\n```")
        elif element.category == "ListItem":
            markdown_sections.append(f"- {text}")
        elif element.category == "NarrativeText":
            markdown_sections.append(text)
        else:
            markdown_sections.append(text)

    content = "\n\n".join(markdown_sections)
    return clean_text_for_llm(content)


def detect_csv_separator(csv_text: str) -> str:
    """Detect the separator used in CSV content."""
    try:
        # Use csv.Sniffer to detect delimiter
        sniffer = csv.Sniffer()
        # Take first few lines for detection
        sample = '\n'.join(csv_text.split('\n')[:10])
        dialect = sniffer.sniff(sample, delimiters=';,\t|')
        return dialect.delimiter
    except:
        # Common separators in order of preference for Polish data
        separators = [';', ',', '\t', '|']
        lines = csv_text.split('\n')[:5]  # Check first 5 lines
        
        for sep in separators:
            counts = [line.count(sep) for line in lines if line.strip()]
            if counts and all(c > 0 and c == counts[0] for c in counts):
                return sep
        
        # Default fallback
        return ','


async def parse_json_content(json_text: str) -> str:
    """Parse JSON content."""
    try:
        parsed = json.loads(json_text)
        return json.dumps(parsed, indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        return json_text


async def parse_pdf_content(pdf_bytes: bytes) -> str:
    """Extract structured content from PDF using Unstructured."""
    try:
        elements = partition_pdf(file=io.BytesIO(pdf_bytes), languages=["polish"])
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


async def parse_csv_content(csv_text: str) -> str:
    """Parse CSV content using Unstructured and format as clean markdown table."""
    try:
        # Detect separator
        separator = detect_csv_separator(csv_text)
        elements = partition_csv(text=csv_text, encoding="utf-8", csv_args={"delimiter": separator})
        
        # Convert elements to clean markdown
        table_text = ""
        for element in elements:
            if hasattr(element, 'text') and element.text.strip():
                table_text += element.text + "\n"
        
        return clean_text_for_llm(table_text) if table_text else csv_text
        
    except Exception as e:
        # Final fallback: return as code block
        return f"```csv\n{csv_text}\n```"


async def parse_xml_content(xml_text: str) -> str:
    """Extract structured content from XML using Unstructured."""
    try:
        xml_bytes = xml_text.encode('utf-8')
        xml_file = io.BytesIO(xml_bytes)
        elements = partition(file=xml_file, content_type="application/xml")
        return elements_to_markdown(elements)
    except Exception as e:
        return f"Error parsing XML: {str(e)}"



# if __name__ == "__main__":
#     format_resource_id = {
#         "csv": [17243],
#         "tsv": [40769, 56651],
#         "json": [29745, 29730, 29728],
#         "geojson": [44008],
#         "jsonld": [51836],
#         "pdf": [2228, 36],
#         "docx": [62358],
#         "doc": [35783, 1375],
#         "html": [],
#         "txt": [21674, 28002, 53255],
#         "xls": [25657, 6044, 5202],
#         "xlsx": [63723, 725, 703],
#         "xml": [46153]
#     }
#     import asyncio
#     x = asyncio.run(get_file_content(format_resource_id["xml"]))
#     for k, v in x.items():
#         print(k, v)