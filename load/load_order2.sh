#!/bin/bash

cqlsh <<EOF
USE d8;

TRUNCATE order2;

COPY order2(o_w_id, o_d_id, o_id, o_c_id, c_first, c_middle, c_last, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d,ol_delivery_d, ols) FROM 'cassandra/order2.csv' WITH DELIMITER = ',' AND NULL = 'null';

quit
EOF
