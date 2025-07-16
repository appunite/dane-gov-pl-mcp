"""
Download file as stream
"""

import requests
import time

file_url = "https://api.dane.gov.pl/media/resources/20190107/Lista_projektow_FE_2014_2020_181231.csv"

with requests.get(file_url, stream=True) as response:
    response.raise_for_status()
    for chunk in response.iter_content(chunk_size=8192):
        chunk_str = chunk.decode("utf-8", errors="ignore")
        print(chunk_str)
        time.sleep(1)

