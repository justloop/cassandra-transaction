#!/bin/bash

cqlsh <<EOF
USE d8;

TRUNCATE warehouse;

TRUNCATE district;

TRUNCATE customer;

TRUNCATE order2;

TRUNCATE item;

COPY warehouse(w_id, w_ytd) FROM 'cassandra/warehouse.csv' WITH DELIMITER = ',' AND NULL = 'null';

COPY district(w_id, d_id, d_next_oid, d_ytd) FROM 'cassandra/district.csv' WITH DELIMITER = ',' AND NULL = 'null';


COPY customer(w_id, d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, o_id) FROM 'cassandra/customer.csv' WITH DELIMITER = ',' AND NULL = 'null';

COPY order2(o_w_id, o_d_id, o_id, o_c_id, c_first, c_middle, c_last, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d,ol_delivery_d, ols) FROM 'cassandra/order2.csv' WITH DELIMITER = ',' AND NULL = 'null';

COPY item(i_w_id, i_id, i_name, i_price, i_im_id, i_data, s_quantity, s_ytd, s_order_cmt, s_remove_cmt,  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data) FROM 'cassandra/item.csv' WITH DELIMITER = ',' AND NULL = 'null';

quit
EOF
