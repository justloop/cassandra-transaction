#!/bin/bash

/temp/apache-cassandra-3.7/bin/cqlsh <<EOF

CREATE KEYSPACE d8 WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};

CREATE TABLE d8.warehouse( w_id int, w_ytd double, PRIMARY KEY(w_id));

CREATE TABLE d8.district( w_id int, d_id int, d_next_oid int, d_ytd double, PRIMARY KEY(w_id, d_id));

CREATE TABLE d8.customer(w_id int, d_id int, c_id int, c_first text, c_middle text, c_last text, c_street_1 text, c_street_2 text, c_city text, c_state text, c_zip text, c_phone text, c_since timestamp, c_credit text, c_credit_lim double, c_discount double, c_balance double, c_ytd_payment double, c_payment_cnt int, c_delivery_cnt int, c_data text, w_name text, w_street_1 text, w_street_2 text, w_city text, w_state text, w_zip text, w_tax double, d_name text, d_street_1 text,  d_street_2 text, d_city text, d_state text, d_zip text, d_tax double, o_id int, PRIMARY KEY(w_id, d_id, c_id));

CREATE TYPE d8.orderline (ol_i_id int,ol_i_name text, ol_amount double,ol_supply_w_id int,ol_quantity int,ol_dist_info text);

CREATE TABLE d8.order2 (o_w_id int, o_d_id int, o_id int, o_c_id int, c_first text, c_middle text, c_last text, o_carrier_id int, o_ol_cnt int, o_all_local int, o_entry_d timestamp,ol_delivery_d timestamp, ols set<frozen <orderline>>, PRIMARY KEY((o_w_id, o_d_id), o_id));

CREATE TABLE d8.item (i_w_id int, i_id int, i_name text, i_price double, i_im_id int, i_data text, s_quantity int, s_ytd double, s_order_cnt int, s_remote_cnt int,  s_dist_01 text, s_dist_02 text, s_dist_03 text, s_dist_04 text, s_dist_05 text, s_dist_06 text, s_dist_07 text, s_dist_08 text, s_dist_09 text, s_dist_10 text, s_data text, PRIMARY KEY(i_w_id, i_id));

CREATE INDEX order_carrier_index ON d8.order2(o_carrier_id);

CREATE MATERIALIZED VIEW d8.top10 AS SELECT c_balance, c_first, c_middle, c_last, w_name, d_name FROM d8.customer WHERE w_id IS NOT NULL AND d_id IS NOT NULL AND c_id IS NOT NULL AND c_balance IS NOT NULL PRIMARY KEY (d_id, c_balance, w_id, c_id) WITH CLUSTERING ORDER BY (c_balance DESC);

quit
EOF
