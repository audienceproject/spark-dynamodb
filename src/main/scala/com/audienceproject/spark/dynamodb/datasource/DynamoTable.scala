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
  * Copyright © 2019 AudienceProject. All rights reserved.
  */
package com.audienceproject.spark.dynamodb.datasource

import java.util

import com.audienceproject.spark.dynamodb.connector.{TableConnector, TableIndexConnector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class DynamoTable(options: CaseInsensitiveStringMap,
                  userSchema: Option[StructType] = None)
    extends Table
        with SupportsRead
        with SupportsWrite {

    private val logger = LoggerFactory.getLogger(this.getClass)

    private val dynamoConnector = {
        val indexName = Option(options.get("indexname"))
        val defaultParallelism = Option(options.get("defaultparallelism")).map(_.toInt).getOrElse(getDefaultParallelism)
        val optionsMap = Map(options.asScala.toSeq: _*)

        if (indexName.isDefined) new TableIndexConnector(name(), indexName.get, defaultParallelism, optionsMap)
        else new TableConnector(name(), defaultParallelism, optionsMap)
    }

    override def name(): String = options.get("tablename")

    override def schema(): StructType = userSchema.getOrElse(inferSchema())

    override def capabilities(): util.Set[TableCapability] =
        Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.ACCEPT_ANY_SCHEMA).asJava

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new DynamoScanBuilder(dynamoConnector, schema())
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
        val parameters = Map(info.options().asScala.toSeq: _*)
        dynamoConnector match {
            case tableConnector: TableConnector => new DynamoWriteBuilder(tableConnector, parameters, info.schema())
            case _ => throw new RuntimeException("Unable to write to a GSI, please omit `indexName` option.")
        }
    }

    private def getDefaultParallelism: Int =
        SparkSession.getActiveSession match {
            case Some(spark) => spark.sparkContext.defaultParallelism
            case None =>
                logger.warn("Unable to read defaultParallelism from SparkSession." +
                    " Parallelism will be 1 unless overwritten with option `defaultParallelism`")
                1
        }

    private def inferSchema(): StructType = {
        val inferenceItems =
            if (dynamoConnector.nonEmpty && options.getBoolean("inferSchema",true)) dynamoConnector.scan(0, Seq.empty, Seq.empty).firstPage().getLowLevelResult.getItems.asScala
            else Seq.empty

        val typeMapping = inferenceItems.foldLeft(Map[String, DataType]())({
            case (map, item) => map ++ item.asMap().asScala.mapValues(inferType)
        })
        val typeSeq = typeMapping.map({ case (name, sparkType) => StructField(name, sparkType) }).toSeq

        if (typeSeq.size > 100) throw new RuntimeException("Schema inference not possible, too many attributes in table.")

        StructType(typeSeq)
    }

    private def inferType(value: Any): DataType = value match {
        case number: java.math.BigDecimal =>
            if (number.scale() == 0) {
                if (number.precision() < 10) IntegerType
                else if (number.precision() < 19) LongType
                else DataTypes.createDecimalType(number.precision(), number.scale())
            }
            else DoubleType
        case list: java.util.ArrayList[_] =>
            if (list.isEmpty) ArrayType(StringType)
            else ArrayType(inferType(list.get(0)))
        case set: java.util.Set[_] =>
            if (set.isEmpty) ArrayType(StringType)
            else ArrayType(inferType(set.iterator().next()))
        case map: java.util.Map[String, _] =>
            val mapFields = (for ((fieldName, fieldValue) <- map.asScala) yield {
                StructField(fieldName, inferType(fieldValue))
            }).toSeq
            StructType(mapFields)
        case _: java.lang.Boolean => BooleanType
        case _: Array[Byte] => BinaryType
        case _ => StringType
    }

}
