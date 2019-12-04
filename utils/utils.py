# -*-coding:utf-8-*-
"""
常用函数
"""
import pandas as pd
import requests
import talib
import tushare as ts
from selenium import webdriver
import sys

sys.path.append('../')
from utils import config

# 当列太多时不换行
pd.set_option('expand_frame_repr', False)
# tushare pro设置token
pro = ts.pro_api(config.TUSHARE_PRO_TOKEN)


def get_cookie(url):
    """
    动态获取cookie
    :param url: 页面链接
    :return:    返回cookie
    """
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    driver = webdriver.Chrome(chrome_options=options)
    # url = 'http://q.10jqka.com.cn/thshy/'
    driver.get(url)
    # 获取cookie列表
    cookies = driver.get_cookies()
    cookie = cookies[0]['value']
    print('最新cookie', cookie)
    driver.close()
    return cookie


def get_page_detail(url, cookie=''):
    """
    获取页面源码
    :param url: 页面链接
    :param cookie: 动态cookie，默认可以不需要
    :return:
    """
    # 模拟浏览器
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/78.0.3904.17 Safari/537.36',
        # 'Cookie': 'v={}'.format(get_cookie())
        'Cookie': 'v={}'.format(cookie)
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.content
        return None
    except BaseException as be:
        print('请求页面失败：', url)
        print(be)
        return None


def stock_basic_tocsv():
    """
    tushare pro查询当前所有正常上市交易的股票列表
    :return:
    """
    stock_basic = pro.stock_basic()
    stock_basic.to_csv(config.STOCK_HISTOTY_PATH + '/stocks.csv', encoding='utf-8-sig', index=False)


def get_stock_basic():
    """
    获取所有股票列表
    读取股票文件，converters将股票代码转为字符串，以免前面的0丢失
    :return:
    """
    try:
        data = pd.read_csv(config.STOCK_HISTOTY_PATH + '/stocks.csv', encoding='utf-8-sig', converters={'symbol': str})
        return data
    except BaseException as be:
        print(be)
        stock_basic_tocsv()
        return get_stock_basic()


def funquan(df, fuquan_type='q'):
    """
    计算复权价
    :param df:
    :param fuquan_type:
    :return:
    """
    # q-前复权,h-后复权
    fq_name = {'h': 0, 'q': -1}
    df.drop_duplicates(subset=['trade_date'], inplace=True)
    df.sort_values(by=['trade_date'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    price1 = df['close'].iloc[fq_name[fuquan_type]]
    # 复权因子
    df['factor'] = (1.0 + df['pct_chg']).cumprod()
    price2 = df['factor'].iloc[fq_name[fuquan_type]]
    df[fuquan_type + '_close'] = df['factor'] * (price1 / price2)

    # 计算复权的开盘价、最高价、最低价
    df[fuquan_type + '_open'] = df['open'] / df['close'] * df[fuquan_type + '_close']
    df[fuquan_type + '_high'] = df['high'] / df['close'] * df[fuquan_type + '_close']
    df[fuquan_type + '_low'] = df['low'] / df['close'] * df[fuquan_type + '_close']


def import_stock_data(ts_code, name):
    """
    导入本地的股票数据
    :param ts_code: 如：000001.SZ
    :param name     如：平安银行
    :return:
    """
    df = pd.read_csv(config.TUSHARE_STOCK_HISTOTY_PATH + '/{}.csv'.format(ts_code + '_' + name), encoding='gbk')
    df.sort_values(by=['trade_date'], inplace=True)
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df.reset_index(inplace=True, drop=True)
    return df


def kdj(df=pd.DataFrame(), fastk_period=9, slowk_period=3, slowd_period=3):
    """
    计算KDJ
    :param df:
    :param fastk_period:    计算RSV的周期
    :param slowk_period:    K线是由RSV的几日移动平均线得到
    :param slowd_period:    D线是由K线的几日移动平均得到
    :return:
    """
    low_list = df['low'].rolling(fastk_period, min_periods=fastk_period).min()
    low_list.fillna(value=df['low'].expanding().min(), inplace=True)
    high_list = df['high'].rolling(fastk_period, min_periods=fastk_period).max()
    high_list.fillna(value=df['high'].expanding().max(), inplace=True)
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100

    # df['k'] = talib.MA(rsv, timeperiod=slowk_period)
    # df['d'] = talib.MA(df['k'], timeperiod=slowd_period)

    df['k'] = pd.DataFrame(rsv).ewm(com=2).mean()
    df['d'] = df['k'].ewm(com=2).mean()
    df['j'] = 3 * df['k'] - 2 * df['d']


def rsi(df=pd.DataFrame(), short=6, long=24):
    """
    RSI超买超卖指标，一般认为小于20为超卖，大于80则为超买
    :param df:  待计算的历史数据
    :param short: 短期市场
    :param long: 长期市场
    :return:
    """
    df[f'rsi_{short}'] = talib.RSI(df['close'].values, timeperiod=short)
    df[f'rsi_{long}'] = talib.RSI(df['close'].values, timeperiod=long)


def get_sina_live_data(codes=[]):
    """
    获取实时股票数据
    :param codes:    个股:sz000001、指数：s_sh000001(加s_),以数组的形式传递
    例如：sh600151,sz000830,s_sh000001,s_sz399001,s_sz399106
    :return:
    """
    code_arr = ','.join(codes)
    # print(code_arr)
    url = f"http://hq.sinajs.cn/list={code_arr}"
    response = get_page_detail(url)
    # print(response)
    if response is not None:
        # 去前后空格，去最后一个分号
        response = response.decode('gbk').strip()[:-1]
        # codes = code.split(',')
        stocks = response.split(';')
        dic = []
        for i in range(0, len(stocks)):
            try:
                # print(stock)
                data = stocks[i][21:-1]
                # print(data)
                listt = data.split(',')
                dictt = {
                    'code': codes[i],       # 股票代码
                    'name': listt[0],       # 0：”大秦铁路”，股票名字；
                    'open': float(listt[1]),       # 1：”27.55″，今日开盘价；
                    'preclose': float(listt[2]),   # 2：”27.25″，昨日收盘价；
                    'current': float(listt[3]),    # 3：”26.91″，当前价格；
                    'high': float(listt[4]),       # 4：”27.55″，今日最高价；
                    'low': float(listt[5]),        # 5：”26.20″，今日最低价；
                    'j_b_1': float(listt[6]),      # 6：”26.91″，竞买价，即“买一”报价；
                    'j_s_1': float(listt[7]),      # 7：”26.92″，竞卖价，即“卖一”报价；
                    'volume': float(listt[8]),     # 8：”22114263″，成交的股票数，由于股票交易以一百股为基本单位，所以在使用时，通常把该值除以一百；
                    'amount': float(listt[9]),     # 9：”589824680″，成交金额，单位为“元”，为了一目了然，通常以“万元”为成交金额的单位，所以通常把该值除以一万；
                    'b_volume_1': float(listt[10]),    # 10：”4695″，“买一”申请4695股，即47手；
                    'b_price_1': float(listt[11]),     # 11：”26.91″，“买一”报价；
                    'b_volume_2': float(listt[12]),    # 12：”57590″，“买二”
                    'b_price_2': float(listt[13]),     # 13：”26.90″，“买二”
                    'b_volume_3': float(listt[14]),    # 14：”14700″，“买三”
                    'b_price_3': float(listt[15]),     # 15：”26.89″，“买三”
                    'b_volume_4': float(listt[16]),    # 16：”14300″，“买四”
                    'b_price_4': float(listt[17]),     # 17：”26.88″，“买四”
                    'b_volume_5': float(listt[18]),    # 18：”15100″，“买五”
                    'b_price_5': float(listt[19]),     # 19：”26.87″，“买五”
                    's_volume_1': float(listt[20]),    # 20：”3100″，“卖一”申报3100股，即31手；
                    's_price_1': float(listt[21]),     # 21：”26.92″，“卖一”报价
                    # (22, 23), (24, 25), (26,27), (28, 29)分别为“卖二”至“卖五的情况”
                    's_volume_2': float(listt[22]),
                    's_price_2': float(listt[23]),
                    's_volume_3': float(listt[24]),
                    's_price_3': float(listt[25]),
                    's_volume_4': float(listt[26]),
                    's_price_4': float(listt[27]),
                    's_volume_5': float(listt[28]),
                    's_price_5': float(listt[29]),
                    'date': listt[30],      # 30：”2008-01-11″，日期；
                    'time': listt[31],      # 31：”15:05:32″，时间；
                    'status': float(listt[32])      # 可能是退市、停牌之类的
                }
            except BaseException:
                continue
            dic.append(dictt)
        return dic
    return None
