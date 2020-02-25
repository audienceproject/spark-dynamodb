package com.audienceproject.spark.dynamodb.datasource

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.audienceproject.spark.dynamodb.connector.{ColumnSchema, TableConnector}

class DynamoBatchDeleter(batchSize: Int,
                         columnSchema: ColumnSchema,
                         connector: TableConnector,
                         client: DynamoDB)
    extends DynamoBatchWriter(batchSize, columnSchema, connector, client) {

    protected override def flush(): Unit = {
        if (buffer.nonEmpty) {
            connector.deleteItems(columnSchema, buffer)(client, rateLimiter)
            buffer.clear()
        }
    }
}
