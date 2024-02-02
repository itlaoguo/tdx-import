#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2024/2/2 10:30
Desc: 导入所有通达信数据到mysql
"""
import os
import configparser
import pymysql
import struct

def getDayFiles(folder_path):
    fileList = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            fileList.append(os.path.join(root, file))

    return fileList

config = configparser.ConfigParser()
config.read('./config.ini')

#连接数据库
connection = pymysql.connect(
    host=config['MYSQL']['HOST'],  # 数据库主机地址
    user=config['MYSQL']['USERNAME'],  # 数据库用户名
    password=config['MYSQL']['PASSWORD'],  # 数据库密码
    database=config['MYSQL']['DATABASE']  # 数据库名称
)
cursor = connection.cursor()

# 通达信日线数据路径
foldePath = 'D:/new_tdx/vipdoc/sh/lday'
dayFiles =  getDayFiles(foldePath)

for dayFile in dayFiles:
    dataSet = []
    with open(dayFile, 'rb') as df:
        buffer = df.read()  # 读取数据到缓存
        size = len(buffer)
        rowSize = 32  # 通信达day数据，每32个字节一组数据
        code = os.path.basename(dayFile).replace('.day', '')
        for i in range(0, size, rowSize):  # 步长为32遍历buffer
            row = list(struct.unpack('IIIIIfII', buffer[i:i + rowSize]))
            # print(row)
            # exit()
            #   [
            #       20150902, //交易日期
            #       302768, //int 开盘价*100
            #       319448,//int 最高价格*100
            #       301909,//int 最低价格*100
            #       316017,//int 收盘价格*100
            #       423262355456.0,//float 成交额度（元）
            #       438170153,//int 成交量（手）
            #       51380508//int 上日收盘价*100
            #    ]

            # row[1] = row[1] / 100
            # row[2] = row[2] / 100
            # row[3] = row[3] / 100
            # row[4] = row[4] / 100
            # row.pop()  # 移除最后无意义字段
            # row.insert(0, code)
            # dataSet.append(row)

            stock = (code,row[0],row[1],row[2],row[3],row[4],row[5],row[6])
            # 写入数据
            insert_query = "INSERT INTO lday (code,date,open,hight,low,close,amount,vol) VALUES (%s, %s,%s, %s,%s, %s,%s, %s)"
            cursor.execute(insert_query, stock)
            sql = cursor.mogrify(insert_query)
            connection.commit()

# 关闭游标和连接
cursor.close()
connection.close()

