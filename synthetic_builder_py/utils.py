from io import BytesIO
from pathlib import Path

import requests
import pandas as pd
import numpy as np

class PATH:
    PACKAGE_DIR = Path(__file__).parent
    CONFIG = PACKAGE_DIR/"config"
    META_JSON_LOCAL = CONFIG/"meta.json"


class URL:
    DOWNLOAD_MARKET_DATA = f"http://192.168.0.25:8080/api/v1/data/download"
    GET_MARKET_DATA = "http://192.168.0.25:8080/api/v1/data/ohlcv"

def download_content(
    url : str, 
    params : dict = None, 
    timeout: int = 30, 
    allow_redirect: bool = False
) -> tuple[int, bytes]:

    response = requests.get(
        url,
        params=params,
        stream=True,
        timeout=timeout,
        allow_redirects=allow_redirect,
    )

    return response.status_code, response.content

def bytes_to_df(
    content : bytes,
) -> pd.DataFrame:
    
    return pd.read_json(BytesIO(content))

def bytes_to_numpy(
    content : bytes,
) -> np.ndarray:
    pass


def move_contract_to_given_next_valid_month(
    contract : str,
    valid_months : str,
) -> str:
    idx = valid_months.find(contract[-3])

    if idx == -1:
        raise

    if idx == len(valid_months) - 1:
        return f"{contract[:-3]}{valid_months[0]}{(int(contract[-2:])+1)%100:02}"

    return f"{contract[:-3]}{valid_months[idx+1]}{contract[-2:]}"

def move_contract_to_given_prev_valid_month(
    contract : str,
    valid_months : str,
) -> str:
    idx = valid_months.find(contract[-3])

    if idx == -1:
        raise

    if idx == 0:
        idx = len(valid_months) -1
        return f"{contract[:-3]}{valid_months[idx]}{(int(contract[-2:])-1)%100:02}"
    else:
        idx -= 1
        return f"{contract[:-3]}{valid_months[idx]}{contract[-2:]}"



if __name__ == "__main__": 
    params = {
        "symbols" : "CLZ08",
        "from" : "1950-01-01",
        "to" : "2010-01-01"
    }
    status_code, content = download_content(URL.GET_MARKET_DATA, params)
    if status_code == 200:
        df = bytes_to_df(content)
        print(df)