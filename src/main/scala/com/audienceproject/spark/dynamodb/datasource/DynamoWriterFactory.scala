package com.audienceproject.spark.dynamodb.datasource

import com.audienceproject.spark.dynamodb.connector.DynamoWritable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class DynamoWriterFactory(batchSize: Int,
                          connector: DynamoWritable,
                          schema: StructType)
    extends DataWriterFactory[InternalRow] {

    override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] =
        new DynamoBatchWriter(batchSize, connector, schema)

}
