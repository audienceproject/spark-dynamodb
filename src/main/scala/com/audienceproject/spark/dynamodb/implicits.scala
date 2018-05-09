package com.audienceproject.spark.dynamodb

import com.audienceproject.spark.dynamodb.reflect.SchemaAnalysis
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Encoder}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object implicits {

    implicit class DynamoDBDataFrameReader(reader: DataFrameReader) {

        def dynamodb(tableName: String): DataFrame =
            getDynamoDBSource(tableName).load()

        def dynamodb(tableName: String, indexName: String): DataFrame =
            getDynamoDBSource(tableName).option("indexName", indexName).load()

        def dynamodbAs[T <: Product : ClassTag : TypeTag](tableName: String): Dataset[T] = {
            implicit val encoder: Encoder[T] = ExpressionEncoder()
            getDynamoDBSource(tableName).schema(SchemaAnalysis[T]).load().as
        }

        def dynamodbAs[T <: Product : ClassTag : TypeTag](tableName: String, indexName: String): Dataset[T] = {
            implicit val encoder: Encoder[T] = ExpressionEncoder()
            getDynamoDBSource(tableName).option("indexName", indexName).schema(SchemaAnalysis[T]).load().as
        }

        private def getDynamoDBSource(tableName: String): DataFrameReader =
            reader.format("com.audienceproject.dynamodb").option("tableName", tableName)

    }

}
