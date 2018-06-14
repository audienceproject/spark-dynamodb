package com.audienceproject.spark.dynamodb.rdd

import com.audienceproject.spark.dynamodb.connector.TableConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

private[dynamodb] class DynamoWriteRelation(data: DataFrame, parameters: Map[String, String])
                                           (@transient val sqlContext: SQLContext)
    extends BaseRelation with Serializable
        with TableScan {

    private val tableName = parameters("tableName")
    private val batchSize = parameters.getOrElse("writeBatchSize", "25").toInt
    private val numPartitions = data.rdd.getNumPartitions
    private val connector = new TableConnector(tableName, numPartitions, parameters)

    override val schema: StructType = data.schema

    override def buildScan(): RDD[Row] = data.rdd

    def write(): Unit = {
        data.foreachPartition(connector.putItems(schema, batchSize) _)
    }

}
