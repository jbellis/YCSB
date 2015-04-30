Yahoo! Cloud System Benchmark (YCSB)
====================================
This organization and fork of YCSB is setup to allow interested parties to keep the YCSB benchmark 
updated as it is observed that the original repository (https://github.com/brianfrankcooper/YCSB) 
is not actively maintained. 

The starting point for this repository is a fork created by 
[jbellis](https://github.com/jbellis/YCSB), which contains many fixes to the original. Vendors who 
wish to join the organization should submit an issue, to keep noise to a minimum it is suggested 
that one developer per vendor is picked as a representative.

A note on comparing multiple systems
------------------------------------

NoSQL systems have widely varying defaults for trading off write durability vs performance.  Make 
sure that you are [comparing apples to apples across all candidates](http://www.datastax.com/dev/blog/how-not-to-benchmark-cassandra-a-case-study).  
The most useful common denominator is synchronously durable writes. The following YCSB clients have 
been verified to perform synchronously durable writes by default:

- Couchbase (using `PersistTo.MASTER`)
- HBase
- MongoDB

Cassandra requires a configuration change in conf/cassandra.yaml.  Uncomment these lines:

    # commitlog_sync: batch
    # commitlog_sync_batch_window_in_ms: 50

Links
-----
- http://research.yahoo.com/Web_Information_Management/YCSB/  
- ycsb-users@yahoogroups.com  

Getting Started
---------------

1. Clone the repository to get the latest and greatest:

    ```sh
    git clone https://github.com/YCSB/YCSB.git
    cd YCSB
    ```

2. Build to generate all needed artifacts:

    ```sh
    mvn package
    ```
    
2. Set up a database to benchmark. There is a README file under each binding 
   directory.

3. Run YCSB command. 
    
    ```sh
    bin/ycsb load basic -P workloads/workloada
    bin/ycsb run basic -P workloads/workloada
    ```

  Running the `ycsb` command without any argument will print the usage. 
   
  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.

  Alternatively, see fabric/README for Thumbtack's work on parallelizing
  YCSB clients using Fabric.
