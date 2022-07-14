from datetime import datetime, timedelta

import futu as ft
import pandas as pd
timedelta(days=1, hours=11, minutes=30)
# code  time_key open close high low  pe_ratio  turnover_rate volume turnover  change_rate  last_close

pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 20)

quote_ctx = ft.OpenQuoteContext(host='127.0.0.1', port=11111)

quote_ctx.start()

TIME_LAYOUT = '%Y-%m-%d %H:%M:%S'
CONCATE_SYMBOL = '____'

market = ft.Market.SH
# code_list = ['SH.600089', 'SH.600036', 'SH.601088']
code_list = ['SH.601985',  'SH.601899', 'SH.600089', 'SH.600584']
begin = '2022-01-01'
end = datetime.now().strftime('%Y-%m-%d')

day_dict = dict()
hour_dict = dict()
for code in code_list:
    ret, data, _ = quote_ctx.request_history_kline(code, begin, end, ft.KLType.K_DAY)
    if ret == ft.RET_OK:
        df1 = pd.DataFrame(data)
        for index, row in df1.iterrows():
            key = row['time_key']
            day_dict.setdefault(row['time_key'], []).append(row)

    ret, data, _ = quote_ctx.request_history_kline(code, begin, end, ft.KLType.K_60M)
    if ret == ft.RET_OK:
        df = pd.DataFrame(data)
        for index, row in df.iterrows():
            key = row['code'] + CONCATE_SYMBOL + row['time_key']
            hour_dict[key] = row

sum = 0.0
for day, rows in day_dict.items():
    df = pd.DataFrame(rows)
    df.reset_index(inplace=True)
    # print(df)
    # 控制取最大涨跌幅还是最小涨跌幅
    idx = df['change_rate'].idxmin()
    code = df.iloc[idx]['code']
    buy = df.iloc[idx]['close']

    time = datetime.strptime(day, TIME_LAYOUT) + timedelta(days=1, hours=10, minutes=30)
    key = code + CONCATE_SYMBOL + time.strftime(TIME_LAYOUT)

    while key not in hour_dict:
        time += timedelta(days=1)
        if time > datetime.now():
            break
        key = code + CONCATE_SYMBOL + time.strftime(TIME_LAYOUT)

    if time > datetime.now():
        print(df)
        break

    sell = hour_dict[key]['close']
    rate = (sell - buy) / buy * 100
    print('{}, buy: {}, sell: {}, rate(%): {:.2f}'.format(key, buy, sell, rate))
    sum += rate
    pass

print('sum rate(%): {:.2f}'.format(sum))

quote_ctx.stop()
