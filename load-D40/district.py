#!/usr/local/bin/python
import csv

with open('cassandra/district.csv', 'wb') as f:  # output csv file
    writer = csv.writer(f)
    with open('D40-data/district.csv', 'r') as districtfile:  # input csv file
        districtdict = csv.DictReader(districtfile, delimiter=',')
        for row in districtdict:
            temp = row["D_W_ID"], row["D_ID"], row["D_NEXT_O_ID"], row["D_YTD"]
            writer.writerow(temp)
