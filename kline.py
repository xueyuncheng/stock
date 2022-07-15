import futu as ft
import pandas as pd
from loguru import logger


def get_kline(quote_ctx, code, start, end):
    datas = []
    page_key = None
    while len(datas) == 0 or page_key is not None:
        ret, data, page_key = quote_ctx.request_history_kline(code, start, end, page_req_key=page_key)
        if ret != ft.RET_OK:
            logger.error("request k line error")
            return
        datas.append(data)
    return pd.concat(datas)


def main():
    pd.set_option('display.width', 400)
    pd.set_option('display.max_columns', 20)

    quote_ctx = ft.OpenQuoteContext(host='127.0.0.1', port=11111)
    quote_ctx.start()

    code = 'SH.600036'
    start = '2021-01-01'
    end = '2022-07-14'

    df = get_kline(quote_ctx, code, start, end)
    print(df)


main()
