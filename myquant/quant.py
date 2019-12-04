# -*-coding:utf-8-*-
import datetime
import threading
import time
import pandas as pd
import requests
# 阻塞式调度器：适用于只跑调度器的程序。特定的时间点触发，只执行一次
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

import sys
sys.path.append('../')

from myquant.client import Client
from utils import bao_stock

""" 策略易实例 """
client = Client(host='localhost', port=8888, key='hushuai', client='title:网上股票交易系统5.0')

class Quant:
    def __init__(self):
        # 启动就登录baostock系统
        bao_stock.login()
        self.today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        # 调度器
        self.scheduler = BlockingScheduler()

        """ 
        持仓
        明细  证券代码  证券名称 股票余额 可用余额 冻结数量  盈亏   成本价 盈亏比(%)  市价  市值 市场代码  交易市场  股东帐户 实际数量 资讯 买入 冻结 卖出冻结 股份实时余额
        0     000859   国风塑业   100      100     0     -35.100  5.701  -6.16   5.350  535.000    1   深Ａ  0272058802  100       0    0    100
        1     600313   农发种业   100      100     0     -1.110   2.711  -0.41   2.700  270.000    2   沪Ａ  A570522883  100       0    0    100
        """
        self.positions = pd.DataFrame()
        """
        账户余额信息
        可用金额  总市值  总资产     资金余额     资金帐户
        1028.44  805     1833.44    1028.44     210400045648
        """
        self.sub_accounts = pd.DataFrame()
        """ 前期的数据，一般是前一天的 """
        self.before_data = pd.DataFrame()

        """ 止损，滑点等 """
        self.pos_amount = 4
        self.max_loss = -0.03
        self.slippage = 0.001
        self.commission = 0.0005

        self.wait_stock = []

    def job(self, msg=''):
        print('标记信息：', msg)
        """
        任务调度
        判断是否是交易日，是则执行自动交易
        :return:
        """
        print('检查交易日：', self.today)
        self.today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        # self.today = '2019-11-29'
        trade_dates = bao_stock.get_trade_dates(start_date=self.today, end_date=self.today)
        if len(trade_dates) == 0 or trade_dates.at[0, 'is_trading_day'] == 0:
            print('今天不是交易日', self.today)
        else:
            # with open('../logo.txt', encoding='utf-8') as f:
            #             #     data = f.read()
            #             #     print(data)
            print('》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》')

            self.before_open()
            print('正常交易日', self.today)
            self.scheduler = BlockingScheduler()
            """ 开盘前运行一次 """
            before_open_trigger = DateTrigger(run_date=f'{self.today} 09:15:00')
            self.scheduler.add_job(func=self.before_open, trigger=before_open_trigger, misfire_grace_time=1000, id='before_open_job')
            """ 开盘运行一次 """
            open_trigger = DateTrigger(run_date=f'{self.today} 09:30:00')
            self.scheduler.add_job(func=self.open, trigger=open_trigger, misfire_grace_time=1000, id='open_job')
            """ 止盈止损 """
            # 在 10:30至11:30期间，每隔1分 运行一次 stop_loss 方法
            am_stop_loss_trigger = IntervalTrigger(minutes=1, start_date=f'{self.today} 10:30:00', end_date=f'{self.today} 11:30:00')
            self.scheduler.add_job(func=self.stop_loss, trigger=am_stop_loss_trigger, misfire_grace_time=1000, id='am_stop_loss_job')
            # 在 13:00至15:00期间，每隔1分 运行一次 stop_loss 方法
            pm_stop_loss_trigger = IntervalTrigger(minutes=1, start_date=f'{self.today} 13:00:00', end_date=f'{self.today} 15:00:00')
            self.scheduler.add_job(func=self.stop_loss, trigger=pm_stop_loss_trigger, misfire_grace_time=1000, id='pm_stop_loss_job')
            """ 收盘前30分钟，运行一次 """
            before_close_trigger = DateTrigger(run_date=f'{self.today} 14:30:00')
            self.scheduler.add_job(func=self.close, trigger=before_close_trigger, misfire_grace_time=1000, id='before_close_job')
            """ 收盘后10分钟，运行一次 """
            close_trigger = DateTrigger(run_date=f'{self.today} 15:10:00')
            self.scheduler.add_job(func=self.close, trigger=close_trigger, misfire_grace_time=1000, id='close_job')
            """ 17:35爬取最新数据，选股，运行一次 """
            select_trigger = DateTrigger(run_date=f'{self.today} 17:35:00')
            self.scheduler.add_job(func=self.select, trigger=select_trigger, misfire_grace_time=1000, id='select_job')
            # 开始任务
            self.scheduler.start()

    def before_open(self):
        """
        开盘前15分钟，可以选出今天要交易的股票
        :return:
        """
        print('即将开盘')
        # 查询资金股份
        positions = client.get_positions()
        print('查询资金股份', positions)
        # 仓位
        self.positions = positions['positions']
        # 账户余额信息
        self.sub_accounts = positions['sub_accounts']
        # 选出要交易的股票,此操作放在收盘后会比较好
        if len(self.wait_stock) == 0:
            # 如果还没有待交易的股票，就重新选一次
            self.select()
        # 此处可以查看集合竞价的情况

    def open(self):
        print(f'{self.today}开盘', datetime.datetime.now())

    def stop_loss(self):
        print('止损监控', datetime.datetime.now())
        for pos in self.positions:
            # 查询个股当前的价位，如果达到止损点，进行止损
            print(pos)
            pass
        # 交易后，查询持仓情况
        self.positions = []

    def before_close(self):
        """
        收盘前30分钟
        :return:
        """
        print('还有半小时收盘', self.today)

    def close(self):
        print('已收盘', datetime.datetime.now())
        # 查询持仓情况
        self.positions = []
        # 统计盈亏
        # 更新个股的目标价，止损将按照这个价进行
        for pos in self.positions:
            # 更新个股的目标价，止损将按照这个价进行
            print(pos)
            pass

    def select(self):
        print('选股', self.today)
        # 更新数据并选出第二天要交易的票
        self.wait_stock = ['000001', '000004']
        # 删除所有调度任务
        print('关闭所有调度任务')
        self.scheduler.remove_all_jobs()
        
    def buy(self, code, price, amount):
        """
        买入
        :param code:买入的股票代码
        :return:
        """
        re = client.buy(client='title:网上股票交易系统5.0', timeout=5, symbol=code, type='LIMIT', priceType=0, price=price, amount=amount)
        print(re)
        pass

    def sell(self, code, price, amount):
        try:
            sell = client.sell(client='title:网上股票交易系统5.0', timeout=5, symbol=code, type='LIMIT', priceType=0, price=price, amount=amount)
            print(type(sell))
            print(sell)
        except requests.exceptions.HTTPError as e:
            print(type(e))
            print(11, e)

    def init(self):
        """
        总调度
        :return:
        """
        print('自动交易启动。。。')
        # 设置调度器,每天9:00运行一次
        run = BlockingScheduler()
        trigger = CronTrigger(day_of_week='*', hour=8, minute=30, end_date='2100-01-01')
        # trigger = CronTrigger(day_of_week='*', hour='9-18', minute='0-59', second=30, end_date='2100-01-01')
        run.add_job(self.job, trigger, max_instances=10000, misfire_grace_time=1000, args=['正式调度'])
        run.start()

    def run(self):
        """
        程序运行入口
        使用线程，让其先运行一次
        :return:
        """
        threading.Thread(target=self.init).start()
        threading.Thread(target=self.job, args=("先执行一次",)).start()


if __name__ == '__main__':
    q = Quant()
    q.run()
    # q.before_open()
    # q.buy('002489', 0.01, 100)
    # q.sell('000859', 6.04, 100)
