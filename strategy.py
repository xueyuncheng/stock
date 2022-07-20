import psycopg2

import pandas as pd
import numpy as np

from backtesting import Backtest, Strategy
from backtesting.lib import crossover

from backtesting.test import SMA


class SmaCross(Strategy):
    n1 = 10
    n2 = 20

    def init(self):
        close = self.data.Close
        self.sma1 = self.I(SMA, close, self.n1)
        self.sma2 = self.I(SMA, close, self.n2)

    def next(self):
        if crossover(self.sma1, self.sma2):
            self.buy()
        elif crossover(self.sma2, self.sma1):
            self.sell()


def get_dataframe(code, start) -> pd.DataFrame:
    dsn = "postgres://postgres:example@localhost:5432/stock"
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute('select code, time_key, open, close, high, low, volume from kline where code = %s and time_key > %s',
                (code, start))
    klines = cur.fetchall()
    df = pd.DataFrame(
        klines, columns=['code', 'time_key', 'Open', 'Close', 'High', 'Low', 'Volume'])
    df.set_index('time_key', inplace=True)
    return df


def main():
    start = '2022-01-01'
    code_dict = {
        'SH.000001': '上证指数',
        'SH.603568': '伟明环保',
        'SH.605028': '世贸能源',
        'SH.600111': '北方稀土',
        'SH.601369': '陕鼓动力',
        'SH.603229': '奥翔药业',
        'SH.600079': '人福医药',
        'SH.600956': '新天绿能',
        'SH.601012': '隆基绿能',
        'SH.601985': '中国核电',
        'SH.601899': '紫金矿业',
        'SH.600584': '长电科技',
    }

    outs = []
    for code, name in code_dict.items():
        # if code != 'SH.603229':
        #     continue
        df = get_dataframe(code, start)
        bt = Backtest(df, SmaCross, cash=10000, exclusive_orders=True)
        output = bt.run()
        # print(code, name, output['Return [%]'],
        #   output['Return (Ann.) [%]'], output['Duration'])
        outs.append(
            (code, name, output['Return [%]'], output['Return (Ann.) [%]'], output['Duration']))
        # bt.plot()

    outdf = pd.DataFrame(
        outs, columns=['code', 'name', 'Return [%]', 'Return (Ann.) [%]', 'Duration'])
    outdf.sort_values('Return [%]', ascending=False, inplace=True)
    print(outdf)


main()
