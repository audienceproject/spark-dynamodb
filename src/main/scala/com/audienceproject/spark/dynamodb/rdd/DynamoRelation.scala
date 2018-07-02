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
package com.audienceproject.spark.dynamodb.rdd

import com.audienceproject.spark.dynamodb.connector.{TableConnector, TableIndexConnector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConverters._

private[dynamodb] class DynamoRelation(userSchema: StructType, parameters: Map[String, String])
                                      (@transient val sqlContext: SQLContext)
    extends BaseRelation with Serializable
        with TableScan with PrunedScan with PrunedFilteredScan {

    private val tableName = parameters("tableName")
    private val indexName = parameters.get("indexName")
    private val numPartitions = parameters.get("readPartitions").map(_.toInt).getOrElse(sqlContext.sparkContext.defaultParallelism)

    private val dynamoConnector =
        if (indexName.isDefined) new TableIndexConnector(tableName, indexName.get, numPartitions, parameters)
        else new TableConnector(tableName, numPartitions, parameters)

    override val schema: StructType = Option(userSchema).getOrElse(inferSchema())

    override val sizeInBytes: Long = dynamoConnector.totalSizeInBytes

    override def buildScan(): RDD[Row] = {
        new DynamoRDD(sqlContext.sparkContext, schema, makePartitions(numPartitions))
    }

    override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
        new DynamoRDD(sqlContext.sparkContext, schema, makePartitions(numPartitions), requiredColumns)
    }

    override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
        new DynamoRDD(sqlContext.sparkContext, schema, makePartitions(numPartitions), requiredColumns, filters)
    }

    override def equals(other: Any): Boolean = {
        other match {
            case that: DynamoRelation =>
                this.tableName == that.tableName &&
                    this.indexName == that.indexName &&
                    this.schema == that.schema &&
                    this.sizeInBytes == that.sizeInBytes
            case _ => false
        }
    }

    private def makePartitions(numPartitions: Int): Seq[ScanPartition] = {
        (0 until numPartitions).map(index => new ScanPartition(schema, index, dynamoConnector))
    }

    private def inferSchema(): StructType = {
        val inferenceItems =
            if (dynamoConnector.nonEmpty) dynamoConnector.scan(0, Seq.empty, Seq.empty).firstPage().getLowLevelResult.getItems.asScala
            else Seq.empty

        val typeMapping = inferenceItems.foldLeft(Map[String, DataType]())({
            case (map, item) =>
                map ++ item.asMap().asScala.mapValues({
                    case number: java.math.BigDecimal =>
                        if (number.scale() == 0) {
                            if (number.precision() < 10) IntegerType
                            else if (number.precision() < 19) LongType
                            else DataTypes.createDecimalType(number.precision(), number.scale())
                        }
                        else DoubleType
                    case _: java.lang.Boolean => BooleanType
                    case _: java.lang.String => StringType
                    case _: java.util.ArrayList[java.util.LinkedHashMap[String,String]] => ArrayType(MapType(StringType,StringType, valueContainsNull = false), containsNull = false)
                    case _: java.util.ArrayList[String] => ArrayType(StringType, containsNull = false)
                    case _: java.util.Map[String,String] => MapType(StringType,StringType, valueContainsNull = false)
                    case unimplementedType => throw new RuntimeException(s"Schema inference not possible, type not implemented: $unimplementedType")
                })
        })
        val typeSeq = typeMapping.map({ case (name, sparkType) => StructField(name, sparkType) }).toSeq

        if (typeSeq.size > 100) throw new RuntimeException("Schema inference not possible, too many attributes in table.")

        StructType(typeSeq)
    }

}
