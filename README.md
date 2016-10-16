# cs4224

## Code structure
Code can be open with Intellij or Eclipse with Java8 (lambda level) and Maven3
```
├── load-D8|D40: the scripts to create/drop keyspace, generate loadable csv from original data, load data into cassandra
    ├── schema: the original data csv schema, we put this header to all the original data
    ├── D8-data: the input data, which contains the schema header as well
    ├── cassandra: the output folder for gen.sh
    ├── create1.sh: script to create keyspace with 1 replication
    ├── create2.sh: script to create keyspace with 3 replication
    ├── drop.sh: script to drop the keyspace
    ├── gen.sh: combined script to generate the loadable data, which invokes all the py files
    ├── load.sh: script to load the generated data into cassandra
    ├── load_table.sh: script to reload the table.csv data into cassandra
    ├── customer.py: script to generate customer.csv to be inserted in cassandra from original tables
    ├── district.py: script to generate district.csv to be inserted in cassandra from original tables
    ├── item.py: script to generate item.csv to be inserted in cassandra from original tables
    ├── order2.py: script to generate order2.csv to be inserted in cassandra from original tables
    ├── warehouse.py: script to generate warehouse.csv to be inserted in cassandra from original tables
├── benchmark: the folder to do benchmarks
    ├── driver_lib: the dependent jar libs
    ├── driver.jar: the compiled java files
    ├── benchmark.py: script to do benchmark by inputing the keyspace and number of clients, it will then generate the benchmark stats
├── benchmark_runner.sh: the single script to benchmark the for all 12 cases
├── src: the java files
    ├── transactions: the file to do individule transactions
    ├── Driver.java: the entry main class to run the transaction by reading from stdin and output to stderr
├── pom.xml: the maven config file
```

## Configuration and run program
1. Compile the program:
    Open with Intellij or Eclipse, import as Maven projects with Java 8 as environment, make sure you can pull the maven dependencies from internet. If you don't need to change code, you can use the compiled code from benchmark folder. To regenerate, you can create a runnable jar from Intellij or Eclipse and get all the jar libs into a separate files. Put the driver.jar (compiled jar) and driver_lib/ (the lib jars) under benchmark/ folder

2. Generate the input files:
    Download the data files, put into corresponding load-D8/D8-data or load-D40/D40-data folder, input the schema header into the first line of each data file
    run gen.sh to generate the table data file in cassandra folder. verify if files are generated

3. Organize the folders:
    upload the files and organized as following
    ```
    ├── load-D8|D40
        ├── cassandra
            ├── the generated data files
        ├── create1.sh
        ├── create3.sh
        ├── drop.sh
        ├── load.sh
    ├── benchmark
        ├── driver_lib
            ├── all the jar libs
        ├── driver.jar
        ├── benchmark.py
    ├── benchmark_runner.sh
    ```
    Note: the cassandra bin is assumed to be at: /temp/apache-cassandra-3.7/bin

4. Run the benchmark_runner
    run> nohup sh benchmark_runner.sh > benchmark.log 2>&1 &

5. Examing the result
    under the root folder of this project
    benchmark-{D8|D40}-{1|3}-{10|20|40}.log is the corresponding results for each benchmark

    under the benchmark folder, we have the {benchmark start timestamp} folder, which contains the benchmark outputs for each client running
