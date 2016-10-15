#!/usr/local/bin/python
import csv

#customer to his last order
customerOrder = dict()
#warehouse id+district id to district
districtTable = dict()
with open('cassandra/customer.csv', 'wb') as f:  # output csv file
    writer = csv.writer(f)
    with open('D40-data/warehouse.csv', 'r') as warehousefile:  # input csv file
        with open('D40-data/district.csv', 'r') as districtfile:  # input csv file
            with open('D40-data/customer.csv', 'r') as customerfile:  # input csv file
                with open('D40-data/order.csv', 'r') as orderfile:  # input csv file
                    warehousedict = csv.DictReader(warehousefile, delimiter=',')
                    warehouselist = list(warehousedict)
                    districtdict = csv.DictReader(districtfile, delimiter=',')
                    districtlist = list(districtdict)
                    customerdict = csv.DictReader(customerfile, delimiter=',')
                    orderdict = csv.DictReader(orderfile, delimiter=',')

                    for order in orderdict:
                        customerOrder[order["O_W_ID"]+","+order["O_D_ID"]+","+order["O_C_ID"]] = order["O_ID"]

                    for district in districtlist:
                        districtTable[district["D_W_ID"]+","+district["D_ID"]] = district

                    for customer in customerdict:
                        district = districtTable[customer['C_W_ID']+","+customer['C_D_ID']]
                        warehouse = warehouselist[int(customer['C_W_ID']) - 1]
                        temp = customer["C_W_ID"], customer["C_D_ID"], customer["C_ID"], customer["C_FIRST"], customer["C_MIDDLE"], customer["C_LAST"], customer["C_STREET_1"], customer["C_STREET_2"], customer["C_CITY"], customer["C_STATE"], customer["C_ZIP"], customer["C_PHONE"], customer["C_SINCE"], customer["C_CREDIT"], customer["C_CREDIT_LIM"], customer["C_DISCOUNT"], customer["C_BALANCE"], customer["C_YTD_PAYMENT"], customer["C_PAYMENT_CNT"], customer["C_DELIVERY_CNT"], customer["C_DATA"], warehouse["W_NAME"], warehouse["W_STREET_1"], warehouse["W_STREET_2"], warehouse["W_CITY"], warehouse["W_STATE"], warehouse["W_ZIP"], warehouse["W_TAX"], district["D_NAME"], district["D_STREET_2"], district["D_STREET_2"], district["D_CITY"], district["D_STATE"], district["D_ZIP"], district["D_TAX"], customerOrder[customer["C_W_ID"]+","+customer["C_D_ID"]+","+customer["C_ID"]]
                        writer.writerow(temp)
