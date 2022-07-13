import futu as ft
import pandas as pd

# code  time_key open close high low  pe_ratio  turnover_rate volume turnover  change_rate  last_close

pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 20)

quote_ctx = ft.OpenQuoteContext(host='127.0.0.1', port=11111)

quote_ctx.start()

market = ft.Market.SH
code_list = ['SH.600111', 'SH.603127', 'SH.601088']
begin = '2022-07-11'
end = '2022-07-13'

day_dict = dict()
hour_dict = dict()
for code in code_list:
    ret, data, _ = quote_ctx.request_history_kline(code, begin, end, ft.KLType.K_DAY)
    if ret == ft.RET_OK:
        df = pd.DataFrame(data)
        df.reset_index()
        for index, row in df.iterrows():
            key = row['time_key']
            day_dict.setdefault(row['time_key'], []).append(row)

    ret, data, _ = quote_ctx.request_history_kline(code, begin, end, ft.KLType.K_60M)
    if ret == ft.RET_OK:
        df = pd.DataFrame(data)
        df.reset_index()
        for index, row in df.iterrows():
            key = row['code'] + row['time_key']
            hour_dict.setdefault(key, [].append(row))

for day, rows in day_dict.items():
    print(pd.DataFrame(rows))
    pass


quote_ctx.stop()
