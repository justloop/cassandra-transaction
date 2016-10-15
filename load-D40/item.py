#!/usr/local/bin/python
import csv

with open('cassandra/item.csv', 'wb') as f:  # output csv file
    writer = csv.writer(f)
    with open('D40-data/item.csv', 'r') as itemfile:  # input csv file
        with open('D40-data/stock.csv', 'r') as stockfile:  # input csv file
            itemdict = csv.DictReader(itemfile, delimiter=',')
            itemlist = list(itemdict)
            stockdict = csv.DictReader(stockfile, delimiter=',')

            for stock in stockdict:
                item = itemlist[int(stock['S_I_ID']) - 1]
                temp = stock["S_W_ID"], item["I_ID"], item["I_NAME"], item["I_PRICE"], item["I_IM_ID"], item["I_DATA"],  stock["S_QUANTITY"], stock["S_YTD"], stock["S_ORDER_CNT"], stock["S_REMOTE_CNT"], stock["S_DIST_01"], stock["S_DIST_02"], stock["S_DIST_03"], stock["S_DIST_04"], stock["S_DIST_05"], stock["S_DIST_06"], stock["S_DIST_07"], stock["S_DIST_08"], stock["S_DIST_09"], stock["S_DIST_10"], stock["S_DATA"]
                writer.writerow(temp)
