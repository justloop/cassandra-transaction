#!/bin/bash

/temp/apache-cassandra-3.7/bin/cqlsh <<EOF
USE d40;

TRUNCATE customer;

COPY customer(w_id, d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, o_id) FROM 'cassandra/customer.csv' WITH DELIMITER = ',' AND NULL = 'null';

quit
EOF
