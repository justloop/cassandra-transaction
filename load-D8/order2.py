#!/usr/local/bin/python
import csv
# store the result row
delivery = dict()
orderToline = dict()
with open('cassandra/order2.csv', 'wb') as f:  # output csv file
    writer = csv.writer(f)
    with open('D8-data/order-line.csv', 'r') as orderlinefile:  # input csv file
        with open('D8-data/order.csv', 'r') as orderfile:  # input csv file
            with open('D8-data/customer.csv', 'r') as customerfile:  # input csv file
                with open('D8-data/item.csv', 'r') as itemfile:  # input csv file
                    orderlinedict = csv.DictReader(orderlinefile, delimiter=',')
                    orderdict = csv.DictReader(orderfile, delimiter=',')
                    itemdict = csv.DictReader(itemfile, delimiter=',')
                    itemlist = list(itemdict)
                    customerdict = csv.DictReader(customerfile, delimiter=',')
                    customerlist = list(customerdict)
                    
                    for orderline in orderlinedict:
                        item = itemlist[int(orderline['OL_I_ID'])-1]
                        orderlineobj = '{ol_i_id: ' + '\'' + orderline['OL_I_ID'] + '\', ol_i_name:\'' + item['I_NAME'] + '\', ol_amount:\'' + orderline['OL_AMOUNT'] + '\', ol_supply_w_id:\'' + orderline['OL_SUPPLY_W_ID'] + '\', ol_quantity:\'' + orderline['OL_QUANTITY'] + '\', ol_dist_info:\'' + orderline['OL_DIST_INFO'] + '\'}'
                        if (orderline['OL_W_ID']+","+orderline['OL_D_ID']+","+orderline['OL_O_ID']) in orderToline:
                            orderToline[(orderline['OL_W_ID']+","+orderline['OL_D_ID']+","+orderline['OL_O_ID'])].add(orderlineobj)
                        else:
                            orderToline[(orderline['OL_W_ID']+","+orderline['OL_D_ID']+","+orderline['OL_O_ID'])] = set()
                            orderToline[(orderline['OL_W_ID']+","+orderline['OL_D_ID']+","+orderline['OL_O_ID'])].add(orderlineobj)
                            delivery[(orderline['OL_W_ID']+","+orderline['OL_D_ID']+","+orderline['OL_O_ID'])] = orderline['OL_DELIVERY_D']

                    print "customer rows =\n", len(customerlist)
                    for order in orderdict:
                        #print "order =\n", order
                        customer = customerlist[int(order['O_C_ID']) - 1]
                        orderlineStr = '{'
                        for obj in orderToline[order['O_W_ID']+","+order['O_D_ID']+","+order['O_ID']]:
                            orderlineStr += obj + ","
                        if len(orderlineStr) > 1:
                            orderlineStr = orderlineStr.strip(',')
                        orderlineStr += '}'
			carrier_id = order['O_CARRIER_ID']
			if carrier_id == 'null':
				carrier_id = '-1'
                        temp = order['O_W_ID'], order['O_D_ID'], order['O_ID'], order['O_C_ID'], customer['C_FIRST'], customer['C_MIDDLE'], customer['C_LAST'], carrier_id, order['O_OL_CNT'], order['O_ALL_LOCAL'], order['O_ENTRY_D'], delivery[(order['O_W_ID']+","+order['O_D_ID']+","+order['O_ID'])], orderlineStr
                        #print "temp =\n", temp
                        writer.writerow(temp)
