/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  *
  * Copyright © 2018 AudienceProject. All rights reserved.
  */
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
            reader.format("com.audienceproject.spark.dynamodb").option("tableName", tableName)

    }

}
