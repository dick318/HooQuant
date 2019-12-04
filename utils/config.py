# -*-coding:utf-8-*-
import os

# tushare pro token
TUSHARE_PRO_TOKEN = '911c92ef77a3c5c5972ae8a5e27d7e3511f5b42228c949b6db011210'

'''项目中的各种数据保存路径'''
# # 项目根目录的地址
current_file = os.path.abspath(os.path.dirname(__file__))
# 程序根目录地址，os.pardir：父目录
root_path = os.path.abspath(os.path.join(current_file, os.pardir))

# 所有股票行情数据
STOCK_HISTOTY_PATH = os.path.abspath(os.path.join(root_path, 'data', 'stocks'))
# 临时输出
OUTPUT_PATH = os.path.abspath(os.path.join(root_path, 'data', 'output'))

# tushare历史行情数据
TUSHARE_STOCK_HISTOTY_PATH = os.path.abspath(os.path.join(STOCK_HISTOTY_PATH, 'tushare'))
# bao_stock历史行情数据
BAO_STOCK_HISTOTY_PATH = os.path.abspath(os.path.join(STOCK_HISTOTY_PATH, 'baostock'))
# 交易记录
DEAL_DATA = os.path.abspath(os.path.join(root_path, 'dealdata'))


if not os.path.exists(STOCK_HISTOTY_PATH):
    os.makedirs(STOCK_HISTOTY_PATH)
if not os.path.exists(TUSHARE_STOCK_HISTOTY_PATH):
    os.makedirs(TUSHARE_STOCK_HISTOTY_PATH)
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)
if not os.path.exists(BAO_STOCK_HISTOTY_PATH):
    os.makedirs(BAO_STOCK_HISTOTY_PATH)
if not os.path.exists(DEAL_DATA):
    os.makedirs(DEAL_DATA)

# print(current_file,root_path)
