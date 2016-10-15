#!/bin/bash
echo "start benchmark runner"

for data in "D8" "D40"
do
	for replica in 1 3
	do
		for clients in 10 20 40
		do
			echo "Initialize DB with ${replica} replication"
			echo "cd load-${data} ; sh drop.sh ; sleep 5 ;sh create${replica}.sh ; sleep 5 ; sh load.sh"
			cd load-${data} ; sh drop.sh ; sleep 5 ;sh create${replica}.sh ; sleep 5 ; sh load.sh
			sleep 5;
			echo "cd benchmark ; python benchmark.py ${data} ${clients} > ../benchmark-${data}-${replica}-${clients}.log"
			cd benchmark ; python benchmark.py ${data} ${clients} > ../benchmark-${data}-${replica}-${clients}.log
		done
	done
done
