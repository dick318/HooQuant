# -*-coding:utf-8-*-
"""
同花顺模拟交易
"""
import pandas as pd
import talib
import datetime
import os
import sys
sys.path.append('../')

from myquant.quant import Quant
from utils import bao_stock, config
from utils.utils import get_sina_live_data, kdj, rsi
from myquant.client import Client

""" 策略易实例 """
client = Client(host='localhost', port=8888, key='hushuai', client='title:网上股票交易系统5.0')


class Test(Quant):

    def stop_loss(self):
        if len(self.positions) == 0:
            self.get_positions()
        # print(self.positions)
        if len(self.before_data) == 0:
            if not os.path.exists(config.DEAL_DATA + '/test.csv'):
                return
            test_df = pd.read_csv(config.DEAL_DATA + '/test.csv', encoding='utf-8-sig', converters={'证券代码': str})
            today = datetime.date.today()
            start_date = today + datetime.timedelta(-15)
            start_date = start_date.strftime('%Y-%m-%d')
            trade_dates = bao_stock.get_trade_dates(start_date=start_date, end_date=self.today)
            before_date = trade_dates.iloc[-2]['calendar_date']
            # print(before_date)
            self.before_data = test_df[test_df['date'] == before_date]
            # print('前一天的数据', self.before_data)

        sell_bn = False
        for pos in self.positions.iterrows():
            before = self.before_data[self.before_data['证券代码'] == pos[1]['证券代码']]
            if before.iloc[0]['可用余额'] <= 100:
                continue
            # print('前一天的数据这只股票', before)
            code = 'sz' if pos[1]['交易市场'] == '深圳Ａ股' else 'sh'
            now_df = get_sina_live_data([code + pos[1]['证券代码']])
            if now_df is None:
                continue
            now_df = now_df[0]
            if ((now_df['current'] - before.iloc[0]['市价']) / before.iloc[0]['市价']) < self.max_loss:
                print('超出最大亏损，卖出', pos)
                price = round(now_df['current'] * (1 + self.slippage), 2)
                self.sell(code=pos[1]['证券代码'], price=price, amount=before.iloc[0]['可用余额'])
                sell_bn = True

        if sell_bn is True:
            self.get_positions()

    def get_positions(self):
        # 查询资金股份
        positions = client.get_positions()
        # 仓位
        self.positions = positions['positions']
        self.positions.drop(labels=['明细', '资讯'], axis=1, inplace=True)
        # self.positions['date'] = '2019-12-03'
        # self.positions.to_csv(config.DEAL_DATA + '/test.csv', encoding='utf-8-sig', index=None)
        # 账户余额信息
        self.sub_accounts = positions['sub_accounts']
        print(self.sub_accounts)
        # self.sub_accounts.to_csv(config.DEAL_DATA + '/sub_accounts.csv', encoding='utf-8-sig', index=None)

    def before_close(self):

        if (self.pos_amount - len(self.positions)) > 0:
            history = bao_stock.get_last_data(days_ago=40, fuquan=2)
            history['code'] = history['code'].apply(lambda x: x.replace('.', ''))
            codes = list(set(history['code'].tolist()))
            dd = []
            for i in range(0, len(codes), 500):
                # 新浪获取实时数据不能一次获取全部，这里每次获取500只股票的数据
                dd = dd + get_sina_live_data(codes[i:i+500])

            current = pd.DataFrame(dd)
            # 选出有用的列
            current = current[['date', 'code', 'open', 'high', 'low', 'current', 'volume', 'amount', 'preclose']]
            current.rename(columns={'current': 'close'}, inplace=True)
            print(type(current.at[0, 'open']))
            # 添加到历史数据中
            history = history.append(current, ignore_index=True, sort=False)
            history.sort_values(by=['code', 'date'], inplace=True)
            # history['turn'] = history['turn'].apply(lambda x : x if x is np.nan else history['turn'].shift(1) * (history['volume'] / history['volume'].shift(1)))
            # history['turn'] = history['turn'] if history['turn'] is np.nan else history['turn'].shift(1) * (history['volume'] / history['volume'].shift(1))
            history['turn'].fillna(value=history['turn'].shift(1) * history['volume'] / history['volume'].shift(1), inplace=True)
            history['pctChg'].fillna(value=(history['close'] - history['preclose']) / history['preclose'], inplace=True)
            history.to_csv(config.OUTPUT_PATH + '/8888.csv', encoding='utf-8-sig', index=None)
            print(history)
            df = history
            # 将时间字符串转换成datetime格式
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
            # 计算kdj
            kdj(df)
            # 计算rsi
            rsi(df)
            # 动量
            df['mom'] = talib.MOM(df['close'], timeperiod=20)
            # 波动率
            df['atr'] = talib.ATR(df.high, df.low, df.close, timeperiod=20)
            # macd
            df['dif'], df['dea'], df['macd'] = talib.MACD(df['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
            # SAR抛物线
            df['sar'] = talib.SAR(df['high'], df['low'], acceleration=0.02, maximum=0.2)
            # ADX
            df['adx'] = talib.ADX(df.high, df.low, df.close, timeperiod=20)
            # 多均线
            df['ma_5'] = df['close'].rolling(5, min_periods=1).mean()
            df['ma_10'] = df['close'].rolling(10, min_periods=1).mean()
            df['ma_15'] = df['close'].rolling(15, min_periods=1).mean()
            df['ma_50'] = df['close'].rolling(50, min_periods=1).mean()
            df['ma_100'] = df['close'].rolling(100, min_periods=1).mean()

            # macd买入信号
            condition1 = (df['dif'] > df['dea'])
            condition2 = (df['dif'].shift(1) < df['dea'].shift(1))
            df.loc[condition1 & condition2, 'macd_b'] = 1

            # macd卖出信号
            condition3 = (df['dif'] < df['dea'])
            condition4 = (df['dif'].shift(1) > df['dea'].shift(1))
            df.loc[condition3 & condition4, 'macd_s'] = 1

            # kdj买入信号
            condition5 = (df['k'] > df['d'])
            condition6 = (df['k'].shift(1) < df['d'].shift(1))
            df.loc[condition5 & condition6, 'kdj_b'] = 1

            # kdj卖出信号
            condition7 = (df['k'] < df['d'])
            condition8 = (df['k'].shift(1) > df['d'].shift(1))
            df.loc[condition7 & condition8, 'kdj_s'] = 1

            # rsi买入信号
            condition5 = (df['rsi_6'] > df['rsi_24'])
            condition6 = (df['rsi_6'].shift(1) < df['rsi_24'].shift(1))
            df.loc[condition5 & condition6, 'rsi_b'] = 1

            # rsi卖出信号
            condition7 = (df['rsi_6'] < df['rsi_24'])
            condition8 = (df['rsi_6'].shift(1) > df['rsi_24'].shift(1))
            df.loc[condition7 & condition8, 'rsi_s'] = 1

            # sar买入信号
            condition9 = (df['close'] > df['sar'])
            condition10 = (df['close'].shift(1) < df['sar'].shift(1))
            df.loc[condition9 & condition10, 'sar_b'] = 1

            # sar卖出信号
            condition11 = (df['close'] < df['sar'])
            condition12 = (df['close'].shift(1) > df['sar'].shift(1))
            df.loc[condition11 & condition12, 'sar_s'] = 1

            # 金叉多均线
            condition13 = (df['close'] > df['ma_5'])
            condition14 = (df['close'] > df['ma_10'])
            condition15 = (df['close'] > df['ma_15'])
            condition16 = (df['close'].shift(1) < df['ma_5'].shift(1))
            condition17 = (df['close'].shift(1) < df['ma_10'].shift(1))
            condition18 = (df['close'].shift(1) < df['ma_15'].shift(1))
            df.loc[condition13 & condition14 & condition15 & condition16 & condition17 & condition18, 'ma3_b'] = 1
            """ 开始筛选 """

            today_df = df[df['date'] == self.today]     # 今天的
            today_df = today_df[today_df['kdj_b'] == 1]  # 有信号的
            today_df = today_df[today_df['close'] < 20]  # 价格小于20 的
            today_df = today_df[today_df['pctChg'] < 9]  # 涨幅不超过9
            today_df.sort_values(by=['turn'], ascending=False, inplace=True)    # 按换手率排序

            # 可用仓位
            empty_pos = self.pos_amount - len(self.positions)
            # 获取余额
            accounts = self.sub_accounts.iloc[0]['可用金额']
            # 获取可能需要的手续费
            commission = accounts * self.commission
            # 每个仓位的钱
            one_money = (accounts - commission) / empty_pos
            print(self.sub_accounts)
            print(self.sub_accounts)
            today_df = today_df[:empty_pos]
            for c in today_df.iterrows():
                now_df = get_sina_live_data([c['code'].replace('.', '')])
                if now_df is None:
                    continue
                now_df = now_df[0]
                buy_money = round(now_df['current'] * (1 + self.slippage), 2)
                buy_num = one_money / buy_money
                buy_num = int(buy_num / 100) * 100
                self.buy(code=c['code'].split('.')[1], price=buy_money, amount=buy_num)

            self.get_positions()

    def close(self):
        self.positions = pd.DataFrame()
        self.sub_accounts = pd.DataFrame()
        self.before_data = pd.DataFrame()
        self.get_positions()
        if not os.path.exists(config.DEAL_DATA + '/test.csv'):
            self.positions.to_csv(config.DEAL_DATA + '/test.csv', encoding='utf-8-sig', index=None)
        else:
            self.positions.to_csv(config.DEAL_DATA + '/test.csv', encoding='utf-8-sig', index=None, header=None, mode='a')


if __name__ == '__main__':
    t = Test()
    t.pos_amount = 10
    t.run()
    # t.get_positions()
    # print(t.sub_accounts.iloc[0]['可用金额'])
