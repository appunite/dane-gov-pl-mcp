from typing import Optional, Literal
import re
from pathlib import Path

import httpx
import polars as pl
from pydantic import BaseModel, Field, field_validator

from src.utils.server_config import mcp, _TIMEOUT, TABULAR_FORMATS
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
async def get_tabular_data(resource_id: int, search_filters: TabularDataFilters) -> dict:
    """Search and filter tabular data within a specific resource using advanced query capabilities. 
    Returns metadata and rows as a list of dictionaries. 
    It's useful when grouping or aggregating data isn't necessary."""
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


class DataFrameOperations(BaseModel):
    """Data object for dataframe operations."""
    primary_group: Optional[str] = Field(None, description="Primary grouping column (e.g., 'col1')")
    secondary_group: Optional[str] = Field(None, description="Secondary grouping column (e.g., 'col2')")
    
    aggregations: Optional[list[Literal["count", "sum", "mean", "median", "min", "max", "std", "var"]]] = Field(None, description="List of aggregation functions to apply")
    aggregation_columns: Optional[list[str]] = Field(None, description="List of columns to aggregate (must match length of aggregations)")
    
    filters: Optional[str] = Field(None, description="Polars filter expression (e.g., 'col1 > 100')")
    
    sort_columns: Optional[list[str]] = Field(None, description="List of columns to sort by (e.g., ['col1', 'col2'] or ['col8_sum', 'col9_mean'])")
    sort_descending: Optional[list[bool]] = Field(None, description="Sort order for each column (True for descending)")
    
    row_limit: Optional[int] = Field(None, description="Maximum number of rows to return")
    
    select_columns: Optional[list[str]] = Field(None, description="Specific columns to select")
    
    @field_validator("row_limit")
    def validate_row_limit(cls, v):
        if v is not None and v < 1:
            raise ValueError("Row limit must be greater than 0")
        return v
    
    @field_validator("sort_columns", "sort_descending") 
    def validate_sort_consistency(cls, v):
        return v
    
    def model_post_init(self, __context):
        if self.aggregations is not None and self.aggregation_columns is not None:
            if len(self.aggregations) != len(self.aggregation_columns):
                raise ValueError("Length of aggregations must match length of aggregation_columns")
        if self.aggregations is not None and self.aggregation_columns is None:
            raise ValueError("aggregation_columns is required when aggregations is specified")
        if self.aggregation_columns is not None and self.aggregations is None:
            raise ValueError("aggregations is required when aggregation_columns is specified")
    

async def _download_file_streaming(url: str, file_path: Path) -> tuple[bool, Exception]:
    """Download file using streaming to handle large files efficiently."""
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            async with client.stream('GET', url, follow_redirects=True) as response:
                response.raise_for_status()
                
                with open(file_path, 'wb') as f:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        f.write(chunk)
                
                return True, None
    except Exception as e:
        return False, e


@mcp.tool()
async def get_tabular_resource_metadata(resource_ids: list[int]) -> dict:
    """Get tabular resources metadata including data schema, headers, count of rows and first row."""
    params = {
        "per_page": 1,
    }
    result = {}
    for id in resource_ids:
        data = await _get(f"/resources/{id}/data", params=params)
        meta = data.get("meta", {})
        data = data.get("data", {})
        row_data = {}
        row_data = {
            "count": meta.get("count", 0),
            "data_schema": meta.get("data_schema", {}),
            "headers_map": meta.get("headers_map", {}),
            "first_row": data[0].get("attributes", {})
        }
        result[id] = row_data
    return result


@mcp.tool()
async def resource_to_dataframe(resource_id: int, dataframe_operations: DataFrameOperations) -> dict:
    """
    Loads tabular resource file into Polars DataFrame with advanced grouping and aggregation capabilities.
    
    Use column names like: col1, col2, col3 (col1=first column, col2=second column, etc.).
    
    For aggregations: aggregations=["sum", "mean"], aggregation_columns=["col8", "col9"] → creates columns "col8_sum", "col9_mean"
    Same column can have multiple aggregations: aggregations=["sum", "mean"], aggregation_columns=["col8", "col8"] → creates "col8_sum", "col8_mean"
    
    After aggregation, original columns are replaced by grouping columns plus aggregated results.
    For sorting after aggregation, use the generated column names (e.g., sort_columns=["col8_sum", "col9_mean"]).
    """
    try:
        # Get resource details to obtain download URL and format
        resource_data = await _get(f"/resources/{resource_id}")
        resource_attrs = resource_data.get("data", {}).get("attributes", {})
        
        download_url = resource_attrs.get("download_url")
        file_format = resource_attrs.get("format", "csv").lower()
        file_size = resource_attrs.get("file_size", 0)
        media_type = resource_attrs.get("media_type")
        
        if not download_url:
            return {"error": "No download URL available for this resource"}
        if media_type != "file":
            return {"error": f"Resource media_type is '{media_type}', expected 'file'"}
        if not resource_data.get("data", {}).get('relationships', {}).get('tabular_data', {}).get('links', {}):
            return {"error": "No tabular data available for this resource"}
        if file_format not in TABULAR_FORMATS:
            return {"error": f"File format is '{file_format}', expected one of {TABULAR_FORMATS}"}  
        
        # Check if file is already cached (any format)
        # Use absolute path to ensure it works in both local and MCP contexts
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        existing_files = list(cache_dir.glob(f"resource_{resource_id}.*"))
        
        if existing_files:
            # Use existing cached file
            cached_file = existing_files[0]
        else:
            # Download new file
            cached_file = cache_dir / f"resource_{resource_id}.{file_format.lower()}"
            
            # Download file using streaming (memory-efficient)
            success, error_msg_download = await _download_file_streaming(download_url, cached_file)
            if not success:
                return {"error": f"Failed to download file: {error_msg_download}\nDownload URL: {download_url}\nDownload path: {cached_file}"}
        
        actual_format = cached_file.suffix[1:].lower()
        
        # Validate the detected format is supported
        if actual_format not in TABULAR_FORMATS:
            return {"error": f"File format is '{file_format}', expected one of {TABULAR_FORMATS}"}
        
        try:
            if actual_format in ['csv', 'tsv']:
                # For CSV/TSV files, use lazy scanning and ignore encoding errors
                # Try different separators to find the best one
                separators_to_try = [',', ';', '|', '\t']
                lf = None
                
                for sep in separators_to_try:
                    try:
                        temp_lf = pl.scan_csv(
                            cached_file,
                            separator=sep,
                            try_parse_dates=True,
                            ignore_errors=True,
                            infer_schema_length=1000,
                            encoding='utf8-lossy'
                        )
                        # Test if this separator gives us multiple columns
                        sample = temp_lf.head(1).collect()
                        if sample.width > 1:  # More than 1 column means separator worked
                            lf = temp_lf
                            break
                    except:
                        continue
                
                # Fallback to comma if no separator worked well
                if lf is None:
                    lf = pl.scan_csv(
                        cached_file,
                        separator=',',
                        try_parse_dates=True,
                        ignore_errors=True,
                        infer_schema_length=1000,
                        encoding='utf8-lossy'
                    )
            elif actual_format in ['xlsx', 'xls']:
                # For Excel files, read and convert to lazy (memory intensive but necessary)
                df = pl.read_excel(cached_file)
                lf = df.lazy()
            elif actual_format == 'json':
                # Try NDJSON first, fallback to regular JSON
                try:
                    lf = pl.scan_ndjson(cached_file)
                except:
                    # Fallback to regular JSON (memory intensive)
                    df = pl.read_json(cached_file)
                    lf = df.lazy()
            else:
                return {"error": f"Unknown format '{actual_format}'"}

        except Exception as e:
            return {"error": f"Could not read file as {actual_format}: {str(e)}. Try checking the file format."}

        def _col_to_name(col_name, df_columns):
            """Convert col1, col2, etc. to actual column names"""
            if isinstance(col_name, str) and col_name.startswith('col'):
                try:
                    col_index = int(col_name[3:]) - 1  # col1 -> 0, col2 -> 1, etc.
                    if 0 <= col_index < len(df_columns):
                        return df_columns[col_index]
                    else:
                        return col_name  # Return as-is if index out of range
                except ValueError:
                    return col_name  # Return as-is if not a valid col pattern
            return col_name

        def _convert_columns(columns, df_columns):
            """Convert col1, col2, etc. to actual column names in lists or single values"""
            if not columns:
                return columns
            if isinstance(columns, str):
                return _col_to_name(columns, df_columns)
            if isinstance(columns, list):
                return [_col_to_name(col, df_columns) for col in columns]
            return columns

        # Get column names for col1, col2, etc. conversion
        df_columns = lf.collect_schema().names()
        
        # Apply operations using Polars lazy API (will use streaming when collect is called)
        try:
            # 1. Column selection (push down projection)
            if dataframe_operations.select_columns:
                column_names = _convert_columns(dataframe_operations.select_columns, df_columns)
                # Use pl.col() with actual column names
                lf = lf.select([pl.col(name) for name in column_names])
            
            # 2. Filtering (push down predicates) 
            if dataframe_operations.filters:
                # Convert col1, col2, etc. to pl.col("actual_column_name") in filter expressions
                filter_expr = dataframe_operations.filters
                
                # Replace col1, col2, etc. with pl.col("actual_column_name")
                def replace_col(match):
                    col_name = match.group(0)
                    actual_name = _col_to_name(col_name, df_columns)
                    return f'col("{actual_name}")'
                
                # Find and replace all col1, col2, etc. patterns
                filter_expr = re.sub(r'\bcol\d+\b', replace_col, filter_expr)
                
                try:
                    # Evaluate the filter expression with converted column references
                    lf = lf.filter(eval(f"pl.{filter_expr}"))
                except Exception as e:
                    return {"error": f"Invalid filter expression '{dataframe_operations.filters}' (converted to: '{filter_expr}'): {str(e)}"}
            
            # 3. Grouping and Aggregation  
            if dataframe_operations.primary_group or dataframe_operations.aggregations:
                group_cols = []
                if dataframe_operations.primary_group:
                    primary_name = _col_to_name(dataframe_operations.primary_group, df_columns)
                    group_cols.append(pl.col(primary_name))
                if dataframe_operations.secondary_group:
                    secondary_name = _col_to_name(dataframe_operations.secondary_group, df_columns)
                    group_cols.append(pl.col(secondary_name))
                
                # Handle aggregations
                if group_cols and dataframe_operations.aggregations and dataframe_operations.aggregation_columns:
                    agg_exprs = []
                    for agg_func, agg_col in zip(dataframe_operations.aggregations, dataframe_operations.aggregation_columns):
                        agg_col_name = _col_to_name(agg_col, df_columns)
                        agg_col_expr = pl.col(agg_col_name)
                        
                        # Create alias with suffix: col8_sum, col9_mean, etc.
                        alias_name = f"{agg_col}_{agg_func}"
                        
                        if agg_func == "count":
                            agg_exprs.append(pl.len().alias(alias_name))
                        elif agg_func == "sum":
                            agg_exprs.append(agg_col_expr.sum().alias(alias_name))
                        elif agg_func == "mean":
                            agg_exprs.append(agg_col_expr.mean().alias(alias_name))
                        elif agg_func == "median":
                            agg_exprs.append(agg_col_expr.median().alias(alias_name))
                        elif agg_func == "min":
                            agg_exprs.append(agg_col_expr.min().alias(alias_name))
                        elif agg_func == "max":
                            agg_exprs.append(agg_col_expr.max().alias(alias_name))
                        elif agg_func == "std":
                            agg_exprs.append(agg_col_expr.std().alias(alias_name))
                        elif agg_func == "var":
                            agg_exprs.append(agg_col_expr.var().alias(alias_name))
                    
                    lf = lf.group_by(group_cols).agg(agg_exprs)
                
                    # Update column names after aggregation since structure has changed
                    df_columns = lf.collect_schema().names()
                
            
            # 4. Sorting
            if dataframe_operations.sort_columns:
                # Convert col1, col2, etc. to actual column names
                sort_col_names = _convert_columns(dataframe_operations.sort_columns, df_columns)
                sort_cols = [pl.col(name) for name in sort_col_names]
                
                descending = dataframe_operations.sort_descending or [False] * len(sort_cols)
                # Ensure descending list matches sort_columns length
                if len(descending) < len(sort_cols):
                    descending.extend([False] * (len(sort_cols) - len(descending)))
                
                lf = lf.sort(sort_cols, descending=descending[:len(sort_cols)])
            
            # 5. Row limiting (this should be done after other operations)
            if dataframe_operations.row_limit:
                lf = lf.head(dataframe_operations.row_limit)
            
            # Step 6: Execute with streaming for large datasets
            df = lf.collect(engine="streaming")
            # Convert to dictionary format for JSON serialization (only if result is small enough)
            if df.height > 10000:  # If more than 10k rows, just return summary
                result_data = df.head(1000).to_dicts()  # Return first 1000 rows as sample
                return {
                    "data": result_data,
                    "shape": df.shape,
                    "columns": df.columns,
                    "note": f"Large result ({df.height:,} rows). Showing first 1,000 rows as sample.",
                    "operations_applied": {
                        "filtering": bool(dataframe_operations.filters),
                        "grouping": bool(dataframe_operations.primary_group or dataframe_operations.secondary_group),
                        "aggregations": dataframe_operations.aggregations,
                        "sorting": bool(dataframe_operations.sort_columns),
                        "row_limit": dataframe_operations.row_limit,
                        "column_selection": bool(dataframe_operations.select_columns)
                    }
                }
            else:
                result_data = df.to_dicts()
                return {
                    "data": result_data,
                    "shape": df.shape,
                    "columns": df.columns,
                    "cached_file": str(cached_file),
                    "file_size_bytes": file_size,
                    "operations_applied": {
                        "filtering": bool(dataframe_operations.filters),
                        "grouping": bool(dataframe_operations.primary_group or dataframe_operations.secondary_group),
                        "aggregations": dataframe_operations.aggregations,
                        "sorting": bool(dataframe_operations.sort_columns),
                        "row_limit": dataframe_operations.row_limit,
                        "column_selection": bool(dataframe_operations.select_columns)
                    }
                }
            
        except Exception as e:
            return {
                "error": f"Error processing DataFrame operations: {str(e)}",
                "suggestion": "Try simpler operations or check column names using get_tabular_resource_metadata first"
            }
    
    except Exception as e:
        return {"error": f"Error accessing resource: {str(e)}"}


# if __name__ == "__main__":
#     import asyncio
#     df_ops = DataFrameOperations(
#         primary_group="col3",
#         aggregations=["sum", "mean", "sum", "mean"],
#         aggregation_columns=["col8", "col8", "col9", "col9"],
#         sort_columns=["col8_sum", "col9_mean"], 
#         sort_descending=[True, True],
#         row_limit=5
#     )
#     # 15274 - xlsx | 3353 - xlsx | 14988 - csv | 65390 - xlsx
#     x = asyncio.run(resource_to_dataframe(65390, df_ops))
#     for i in x['data']:
#         print(i)

    