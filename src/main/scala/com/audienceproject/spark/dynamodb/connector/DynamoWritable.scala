package com.audienceproject.spark.dynamodb.connector

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

trait DynamoWritable {

    val writeLimit: Int

    def putItems(schema: StructType, batchSize: Int)(items: Iterator[Row]): Unit

}
