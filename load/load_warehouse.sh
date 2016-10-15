#!/bin/bash

/temp/apache-cassandra-3.7/bin/cqlsh <<EOF
USE d8;

TRUNCATE warehouse;

COPY warehouse(w_id, w_ytd) FROM 'cassandra/warehouse.csv' WITH DELIMITER = ',' AND NULL = 'null';

quit
EOF
