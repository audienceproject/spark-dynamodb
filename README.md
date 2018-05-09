# Spark+DynamoDB
Plug-and-play implementation of an Apache Spark custom data source for AWS DynamoDB.

## Parameters
The following parameters can be set as options on the Spark reader object before loading.

- `readPartitions` number of partitions to split the initial RDD when loading the data into Spark. Corresponds 1-to-1 with total number of segments in the DynamoDB parallel scan used to load the data. Defaults to `sparkContext.defaultParallelism`
- `targetCapacity` fraction of provisioned read capacity on the table (or index) to consume for reading. Default 1 (i.e. 100% capacity).
- `stronglyConsistentReads` whether or not to use strongly consistent reads. Default false.
- `bytesPerRCU` number of bytes that can be read per second with a single Read Capacity Unit. Default 4000 (4 KB). This value is multiplied by two when `stronglyConsistentReads=false`
