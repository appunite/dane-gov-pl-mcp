from datetime import datetime
import httpx



_API = f"https://api.dane.gov.pl/"
_TIMEOUT = httpx.Timeout(30.0, connect=5.0)


async def _get(path: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        try:
            resp = await client.get(f"{_API}{path}", params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise ValueError(f"dane.gov.pl API failed for {_API}{path}: {e.response.status_code}, {e.response.text}")


def iso_to_unix(iso_string: str) -> int:
    """
    Converts an ISO 8601 formatted date string to a Unix timestamp (seconds since epoch).
    Args:
        iso_string (str): ISO date string like '2025-05-20T12:30:00Z' or '2025-05-20T12:30:00+00:00'
    Returns:
        int: Unix timestamp (seconds since 1970-01-01T00:00:00Z)
    """
    try:
        dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception as e:
        raise ValueError(f"Invalid ISO date string: {iso_string}") from e
