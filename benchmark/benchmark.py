#!//usr/bin/python
from subprocess import Popen
import sys
from time import gmtime, strftime
import glob

folder = strftime("%Y%m%d%H%M%S", gmtime())
if len(sys.argv) != 2:
    print "usage is: benchmark.py keyspace clients_count."
    sys.exit()

arguments = str(sys.argv)
keyspace = arguments[1]
print "Connecting to keyspace: ", keyspace
clientsCount = arguments[2]

print "Starting clients: ", clientsCount
commands = []
for i in range(int(clientsCount)):
    commands.append("java cassandra-0.0.1-SNAPSHOT.jar < " + keyspace +
                    "-xact/" + i + ".txt 2>&1 1>/dev/null > " + folder + "/" + i + ".txt")
procs = [Popen(i) for i in command]

print "Wait until program finishes..."
for p in procs:
    p.wait()

print "Collecting results..."
throughput = []
for filename in glob.glob(folder + "/*.txt"):
    last = ""
    for line in open(filename):
        last = line
    if len(last) == 0:
        print "Err reading output: ", filename
    else
        throughput[i] = float(last.split(':')[-1])
    i + +
max_value = max(throughput)
min_value = min(throughput)
avg_value = sum(throughput) / len(throughput)
print "\nMax throughput = \n", max_value
print "Min throughput = \n", min_value
print "Avg throughput = \n", avg_value
