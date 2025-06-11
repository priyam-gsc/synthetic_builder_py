from datetime import date

import pandas as pd

from .utils import (
    URL,
    download_content,
    bytes_to_df,
)

START_DATE = "1950-01-01"

def get_outright_df(
    contract : str,
    ohlcv : str,
) -> tuple[bool, pd.DataFrame]:
    
    today = date.today()
    end_date = today.strftime('%Y-%m-%d')

    params = {
        "symbols" : contract,
        "from" : START_DATE,
        "to" : end_date
    }

    status_code, content = download_content(
        URL.GET_MARKET_DATA,
        params
    )

    if status_code != 200:
        return False, pd.DataFrame()

    df = bytes_to_df(content)
    df.columns = df.columns.str.lower()

    # way of parsing timestamp from market data api 
    # df.timestamp = pd.to_datetime(df.timestamp.str.replace(" UTC", "", regex=False))
    df.timestamp = pd.to_datetime(df.timestamp)
    
    # column_drop_list = ["sym", "openinterest"]
    column_drop_list = [ "open_int"]
    if "o" not in ohlcv:
        column_drop_list.append("open")
    if "h" not in ohlcv:
        column_drop_list.append("high")
    if "l" not in ohlcv:
        column_drop_list.append("low")
    if "c" not in ohlcv:
        column_drop_list.append("close")
    if "v" not in ohlcv:
        column_drop_list.append("volume")

    df.drop(column_drop_list, axis=1, inplace=True)
    df.set_index(["timestamp"], inplace=True)

    return True, df



if __name__ == "__main__":
    ok, df = get_outright_df("RBZ23", "ohlc")
    if ok:
        print(df)