#!/usr/local/bin/python
import csv

with open('cassandra/warehouse.csv', 'wb') as f:  # output csv file
    writer = csv.writer(f)
    with open('D40-data/warehouse.csv', 'r') as csvfile:  # input csv file
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            temp = row["W_ID"], row["W_YTD"]
            writer.writerow(temp)
