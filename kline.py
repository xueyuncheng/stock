from datetime import timedelta
import futu as ft
import pandas as pd
from loguru import logger
import psycopg2
from psycopg2 import extras


def get_kline(quote_ctx: ft.OpenQuoteContext, code: str, start: str, end: str):
    datas = []
    page_key = None
    while len(datas) == 0 or page_key is not None:
        ret, data, page_key = quote_ctx.request_history_kline(
            code, start, end, page_req_key=page_key)
        if ret != ft.RET_OK:
            logger.error("request k line error")
            return
        datas.append(data)
    return pd.concat(datas)


def get_last(conn, code):
    cur = conn.cursor()
    cur.execute(
        'select time_key from kline where code = %s order by id desc limit 1', (code,))
    last = cur.fetchone()
    cur.close()
    return last


def save_kline(conn, name, df: pd.DataFrame):
    records = []
    for i, row in df.iterrows():
        records.append((row['code'], name, row['time_key'], row['open'], row['close'], row['high'], row['low'],
                        row['pe_ratio'], row['turnover_rate'], row['last_close'], row['change_rate'],
                        row['volume'], row['turnover']))

    insert_query = "insert into kline (code, name, time_key, open, close, high, low, pe_ratio, turnover_rate, " \
                   "last_close, change_rate, volume, turnover) values %s "
    psycopg2.extras.execute_values(conn.cursor(), insert_query, records)
    conn.commit()


def main():
    pd.set_option('display.width', 400)
    pd.set_option('display.max_columns', 20)

    quote_ctx = ft.OpenQuoteContext(host='127.0.0.1', port=11111)
    quote_ctx.start()

    code_dict = {
        'SH.000001': '上证指数',
        'SH.603568': '伟明环保',
        'SH.605028': '世贸能源',
        'SH.601369': '陕鼓动力',
        'SH.603229': '奥翔药业',
        'SH.600079': '人福医药',
        'SH.600956': '新天绿能',
        'SH.601012': '隆基绿能',
        'SH.601985': '中国核电',
        'SH.601899': '紫金矿业',
        'SH.600584': '长电科技',
        'SH.600111': '北方稀土',
    }

    dsn = "postgres://postgres:example@localhost:5432/stock"
    conn = psycopg2.connect(dsn)

    for code, name in code_dict.items():
        start = '2012-08-01'
        end = '2022-08-01'
        last = get_last(conn, code)
        if last is not None:
            start = (last[0] + timedelta(days=1)).strftime('%Y-%m-%d')
        df = get_kline(quote_ctx, code, start, end)
        print(code, name, len(df))
        if len(df) > 0:
            save_kline(conn, name, df)

    conn.close()
    quote_ctx.close()


main()
