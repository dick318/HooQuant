# -*-coding:utf-8-*-
import datetime
import os
import sys
import baostock as bs
import pandas as pd
import tushare as ts

sys.path.append('../')
from utils import utils
from utils import config

sys.path.append('../')

# 当列太多时不换行
pd.set_option('expand_frame_repr', False)
# tushare pro设置token
pro = ts.pro_api(config.TUSHARE_PRO_TOKEN)
today = datetime.date.today().strftime('%Y-%m-%d')


# 登录
def login():
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    if lg.error_code != '0':
        print('login respond  error_msg:'+lg.error_msg)
    # print('login respond error_code:'+lg.error_code)


# 退出登录
def logout():
    # 登出系统
    bs.logout()


# 根据股票代码获取历史数据
def get_stock_history_k(code, start_date, end_date=today, frequency='d', fuquan='3',
                        fields='date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,'
                               'tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST'):
    fuquan = str(fuquan)
    # 获取历史K线数据
    # 详细指标参数，参见“历史行情指标参数”章节
    # frequency="d"取日k线，adjustflag="3"默认不复权
    rs = bs.query_history_k_data_plus(code, fields=fields,
                                      start_date=start_date, end_date=end_date,
                                      frequency=frequency, adjustflag=fuquan)
    if rs.error_code != '0':
        print('query_history_k_data_plus respond error_code::'+rs.error_msg)
        login()
        return get_stock_history_k(code, start_date, end_date, frequency, fuquan, fields)

    # 打印结果集
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    str_to_num(result)
    return result


# 获取沪深300成分股
def get_hs300_stocks(date=today):
    # 获取沪深300成分股
    rs = bs.query_hs300_stocks(date=date)
    if rs.error_code != '0':
        print('query_hs300 respond error_code::'+rs.error_msg)
        login()
        return get_hs300_stocks(date)

    # 打印结果集
    hs300_stocks = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        hs300_stocks.append(rs.get_row_data())
    result = pd.DataFrame(hs300_stocks, columns=rs.fields)
    return result


# 获取上证50成分股
def get_sz50_stocks(date=today):
    # 获取上证50成分股
    rs = bs.query_sz50_stocks(date=date)
    if rs.error_code != '0':
        print('query_sz50 respond error_code::'+rs.error_msg)
        login()
        return get_sz50_stocks(date)

    # 打印结果集
    sz50_stocks = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        sz50_stocks.append(rs.get_row_data())
    result = pd.DataFrame(sz50_stocks, columns=rs.fields)
    return result


# 获取中证500成分股
def get_zz500_stocks(date=today):
    # 获取中证500成分股
    rs = bs.query_zz500_stocks(date=date)
    if rs.error_code != '0':
        print('query_zz500 respond error_code::'+rs.error_msg)
        login()
        return get_zz500_stocks(date)

    # 打印结果集
    zz500_stocks = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        zz500_stocks.append(rs.get_row_data())
    result = pd.DataFrame(zz500_stocks, columns=rs.fields)
    # 结果集输出到csv文件
    return result


def get_stock_industry(code=None, date=None):
    # 获取行业分类数据
    rs = bs.query_stock_industry(code=code, date=date)
    # rs = bs.query_stock_basic(code_name="浦发银行")
    if rs.error_code != '0':
        print('query_stock_industry respond error_code::'+rs.error_msg)
        login()
        return get_stock_industry(code, date)

    # 打印结果集
    industry_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        industry_list.append(rs.get_row_data())
    result = pd.DataFrame(industry_list, columns=rs.fields)
    # 结果集输出到csv文件
    return result


def get_all_stocks(date='2019-11-22'):
    rs = bs.query_all_stock(date)
    if rs.error_code != '0':
        print('query_stock_industry respond error_code::'+rs.error_msg)
        login()
        return get_all_stocks(date)

    # 打印结果集
    industry_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        industry_list.append(rs.get_row_data())
    result = pd.DataFrame(industry_list, columns=rs.fields)
    for s in result.iterrows():
        print(s)
    # 结果集输出到csv文件
    return result

#
def get_last_data(days_ago=1, fuquan=3):
    """
    获取所有股票最后的数据
    :param days_ago: 几天的数据
    :param fuquan: 复权
    :return:
    """
    today1 = datetime.date.today()
    start_date = today1 + datetime.timedelta(-days_ago)
    start_date = start_date.strftime('%Y-%m-%d')
    # 判断是否已经获取过了，存在就直接读取返回
    path = config.OUTPUT_PATH + f'/last_data_{start_date}_to_{today}.csv'

    print(os.path.exists(path), path)
    if os.path.exists(path):
        data = pd.read_csv(path, encoding='utf-8-sig')
        return data
    # 没有就通过接口获取
    print('开始获取最新的行情数据，请耐心等待...')
    stock_basic = pro.stock_basic()
    for index, stock in stock_basic.iterrows():
        if index % 40 == 0:
            print(end='》')
        code = stock['ts_code'].split('.')[1].upper() + '.' + stock['symbol']
        dff = get_stock_history_k(code, start_date=start_date, end_date=today, fuquan=fuquan)
        if not os.path.exists(path):
            dff.to_csv(path, index=None, encoding='utf-8-sig')
        else:
            dff.to_csv(path, index=None, encoding='utf-8-sig', header=None, mode='a')
        # data = data.append(dff, ignore_index=True)
    data = pd.read_csv(path, encoding='utf-8-sig')
    return data


# 获取指数数据
def get_index_history_k(index_code, start_date, end_date=today, frequency='d'):
    rs = bs.query_history_k_data_plus(index_code,
                                      "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                                      start_date=start_date, end_date=end_date, frequency=frequency)

    if rs.error_code != '0':
        print('query_index_history_k_data_plus respond error_code::'+rs.error_msg)
        login()
        return get_index_history_k(index_code, start_date, end_date, frequency)

    # 打印结果集
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    # print(result)
    str_to_num(result, is_index=True)
    # 结果集输出到csv文件
    return result


def get_trade_dates(start_date, end_date=today, is_trading_day=1):
    # 获取交易日信息
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    if rs.error_code != '0':
        print('query_trade_dates respond error_code::'+rs.error_msg)
        login()
        return get_trade_dates(start_date, end_date)

    # 打印结果集
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    result['is_trading_day'] = pd.to_numeric(result['is_trading_day'])
    result = result[result['is_trading_day'] == is_trading_day]
    return result


# 字符串转数值
def str_to_num(data, is_index=False):
    data['open'] = pd.to_numeric(data['open'])
    data['high'] = pd.to_numeric(data['high'])
    data['low'] = pd.to_numeric(data['low'])
    data['close'] = pd.to_numeric(data['close'])
    data['preclose'] = pd.to_numeric(data['preclose'])
    data['volume'] = pd.to_numeric(data['volume'])
    data['amount'] = pd.to_numeric(data['amount'])
    data['pctChg'] = pd.to_numeric(data['pctChg'])
    # 指数没有这些数据
    if is_index is False:
        data['peTTM'] = pd.to_numeric(data['peTTM'])
        data['pbMRQ'] = pd.to_numeric(data['pbMRQ'])
        data['psTTM'] = pd.to_numeric(data['psTTM'])
        data['pcfNcfTTM'] = pd.to_numeric(data['pcfNcfTTM'])
        data['turn'] = pd.to_numeric(data['turn'])
        data['isST'] = pd.to_numeric(data['isST'])


error_index = -1


# 将复权前后的数据整合到h5文件中，key是股票代码
def get_all():
    global error_index
    login()
    try:
        now_date = datetime.date.today().strftime('%m%d')
        stock_basic = utils.get_stock_basic()
        size = len(stock_basic)
        for index, stock in stock_basic.iterrows():
            if index < error_index:
                continue
            error_index = index
            code = stock['ts_code'].split('.')[1].lower() + '.' + stock['symbol']
            print('开始更新第{}/{}个股 {} {}'.format(index + 1, size, stock['ts_code'], stock['name']))
            # kdf1 = get_stock_history_k(code, start_date='2006-01-01', end_date=today, fuquan='1')
            kdf2 = get_stock_history_k(code, start_date='2006-01-01', end_date=today, fuquan='2')
            # kdf3 = get_stock_history_k(code, start_date='2006-01-01', end_date=today, fuquan='3')
            # kdf.to_csv(config.BAO_STOCK_HISTOTY_PATH + f'/{code}.csv', encoding='utf-8-sig', index=None)
            # kdf1.to_hdf(config.BAO_STOCK_HISTOTY_PATH + f'/bao_all_stocks_{now_date}_1.h5',
            #             key=stock['symbol'], mode='a')
            kdf2.to_hdf(config.BAO_STOCK_HISTOTY_PATH + f'/bao_all_stocks_{now_date}_2.h5',
                        key=stock['symbol'], mode='a')
            # kdf3.to_hdf(config.BAO_STOCK_HISTOTY_PATH + f'/bao_all_stocks_{now_date}_3.h5',
            #             key=stock['symbol'], mode='a')

    except BaseException as be:
        print(be)
        get_all()
    logout()


def all_to_h5():
    login()
    stock_basic = utils.get_stock_basic()
    size = len(stock_basic)
    now_date = datetime.date.today().strftime('%m%d')
    # hdf = pd.DataFrame()
    for index, stock in stock_basic.iterrows():
        code = stock['ts_code'].split('.')[1].lower() + '.' + stock['symbol']
        print('开始更新第{}/{}个股 {} {}'.format(index + 1, size, stock['ts_code'], stock['name']))
        kdf = get_stock_history_k(code, start_date='2006-01-01', end_date=today, fuquan='2')
        kdf.to_hdf(config.BAO_STOCK_HISTOTY_PATH + f'/bao_all_stocks_{now_date}_2.h5', key='bao_all_stocks', mode='a')
        # hdf = hdf.append(kdf, ignore_index=True)
    # hdf.to_hdf(config.BAO_STOCK_HISTOTY_PATH + '/bao_all_stocks.h5', key='bao_all_stocks')
    # hdf_all = pd.HDFStore(config.BAO_STOCK_HISTOTY_PATH + '/bao_all_stocks.h5')
    # print(hdf_all)
    logout()


def read_hdfs():
    store = pd.HDFStore(config.BAO_STOCK_HISTOTY_PATH + '/bao_all_stocks_1101.h5')
    print(store)
    print(store.get('bao_all_stocks'))
