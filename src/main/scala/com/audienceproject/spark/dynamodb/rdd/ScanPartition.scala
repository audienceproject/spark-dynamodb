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

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity
import com.audienceproject.spark.dynamodb.connector.DynamoConnector
import org.apache.spark.Partition
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.spark_project.guava.util.concurrent.RateLimiter

import scala.annotation.tailrec
import scala.collection.JavaConverters._

private[dynamodb] class ScanPartition(schema: StructType,
                                      partitionIndex: Int,
                                      connector: DynamoConnector)
    extends Partition with Serializable {

    @transient
    private lazy val typeConversions = schema.collect({
        case StructField(name, dataType, _, _) => name -> TypeConversion(name, dataType)
    }).toMap

    def scanTable(requiredColumns: Seq[String], filters: Seq[Filter]): Iterator[Row] = {

        val rateLimiter = RateLimiter.create(connector.rateLimit.max(1))

        val scanResult = connector.scan(index, requiredColumns, filters)

        val pageIterator = scanResult.pages().iterator().asScala

        new Iterator[Row] {

            var innerIterator: Iterator[Row] = Iterator.empty
            var prevConsumedCapacity: Option[ConsumedCapacity] = None

            @tailrec
            override def hasNext: Boolean = innerIterator.hasNext || {
                if (pageIterator.hasNext) {
                    nextPage()
                    hasNext
                }
                else false
            }

            override def next(): Row = innerIterator.next()

            private def nextPage(): Unit = {
                // Limit throughput to provisioned capacity.
                prevConsumedCapacity
                    .map(capacity => Math.ceil(capacity.getCapacityUnits).toInt)
                    .foreach(rateLimiter.acquire)

                val page = pageIterator.next()
                prevConsumedCapacity = Option(page.getLowLevelResult.getScanResult.getConsumedCapacity)
                innerIterator = page.getLowLevelResult.getItems.iterator().asScala.map(itemToRow(requiredColumns))
            }

        }
    }

    private def itemToRow(requiredColumns: Seq[String])(item: Item): Row =
        if (requiredColumns.nonEmpty) Row.fromSeq(requiredColumns.map(columnName => typeConversions(columnName)(item)))
        else Row.fromSeq(item.asMap().asScala.values.toSeq.map(_.toString))

    override def index: Int = partitionIndex

}
