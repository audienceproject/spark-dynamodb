# Spark+DynamoDB
Plug-and-play implementation of an Apache Spark custom data source for AWS DynamoDB.

## Features

- Distributed, parallel scan with lazy evaluation
- Throughput control by rate limiting on target fraction of provisioned table/index capacity
- Schema discovery to suit your needs
  - Dynamic inference
  - Static analysis of case class
- Column and filter pushdown
- Global secondary index support

## Quick Start Guide

```scala
import com.audienceproject.spark.dynamodb.implicits._

// Load a DataFrame from a Dynamo table. Only incurs the cost of a single scan for schema inference.
val dynamoDf = spark.dynamodb("SomeTableName") // <-- DataFrame of Row objects with inferred schema.

// Scan the table for the first 100 items (the order is arbitrary) and print them.
dynamoDf.show(100)

// Case class representing the items in our table.
import com.audienceproject.spark.dynamodb.attribute
case class Vegetable (name: String, color: String, @attribute("weight_kg") weightKg: Double)

// Load a Dataset[Vegetable]. Notice the @attribute annotation on the case class - we imagine the weight attribute is named with an underscore in DynamoDB.
import org.apache.spark.sql.functions._
import spark.implicits._
val vegetableDs = spark.dynamodbAs[Vegetable]("VegeTable")
val avgWeightByColor = vegetableDs.agg($"color", avg($"weightKg")) // The column is now called 'weightKg' in the Dataset.
```

## Parameters
The following parameters can be set as options on the Spark reader object before loading.

- `readPartitions` number of partitions to split the initial RDD when loading the data into Spark. Corresponds 1-to-1 with total number of segments in the DynamoDB parallel scan used to load the data. Defaults to `sparkContext.defaultParallelism`
- `targetCapacity` fraction of provisioned read capacity on the table (or index) to consume for reading. Default 1 (i.e. 100% capacity).
- `stronglyConsistentReads` whether or not to use strongly consistent reads. Default false.
- `bytesPerRCU` number of bytes that can be read per second with a single Read Capacity Unit. Default 4000 (4 KB). This value is multiplied by two when `stronglyConsistentReads=false`
