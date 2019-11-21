package com.audienceproject.spark.dynamodb.datasource

import com.audienceproject.spark.dynamodb.connector.DynamoWritable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

class DynamoBatchWriter(batchSize: Int,
                        connector: DynamoWritable,
                        schema: StructType)
    extends DataWriter[InternalRow] {

    private val buffer = new ArrayBuffer[InternalRow](batchSize)

    override def write(record: InternalRow): Unit = {
        buffer += record.copy()
        if (buffer.size == batchSize) {
            flush()
        }
    }

    override def commit(): WriterCommitMessage = {
        flush()
        new WriterCommitMessage {}
    }

    override def abort(): Unit = {}

    private def flush(): Unit = {
        connector.putItems(schema, buffer)
        buffer.clear()
    }

}
