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
package com.audienceproject.spark.dynamodb.connector

import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.{ItemCollection, ScanOutcome}
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity

import scala.collection.JavaConverters._

private[dynamodb] class TableIndexConnector(tableName: String, indexName: String, totalSegments: Int, parameters: Map[String, String])
    extends DynamoConnector with Serializable {

    private val consistentRead = parameters.getOrElse("stronglyConsistentReads", "false").toBoolean

    override val (hashKey, rangeKey, rateLimit, itemLimit, totalSizeInBytes) = {
        val table = getClient.getTable(tableName)
        val indexDesc = table.describe().getGlobalSecondaryIndexes.asScala.find(_.getIndexName == indexName).get

        // Key schema.
        val hashKey = indexDesc.getKeySchema.asScala.find(_.getKeyType == "HASH").get.getKeyType
        val rangeKey = indexDesc.getKeySchema.asScala.find(_.getKeyType == "RANGE").map(_.getKeyType)

        // Parameters.
        val bytesPerRCU = parameters.getOrElse("bytesPerRCU", "4000").toInt
        val targetCapacity = parameters.getOrElse("targetCapacity", "1").toDouble
        val readFactor = if (consistentRead) 1 else 2

        // Rate limit calculation.
        val tableSize = indexDesc.getIndexSizeBytes.toDouble
        val avgItemSize = tableSize / indexDesc.getItemCount
        val readCapacity = indexDesc.getProvisionedThroughput.getReadCapacityUnits * targetCapacity

        val rateLimit = (readCapacity / totalSegments).toInt
        val itemLimit = (bytesPerRCU / avgItemSize * rateLimit).toInt * readFactor

        (hashKey, rangeKey, rateLimit, itemLimit, tableSize.toLong)
    }

    override def scan(segmentNum: Int): ItemCollection[ScanOutcome] = {
        val scanSpec = getBaseScanSpec.withSegment(segmentNum)
        getClient.getTable(tableName).scan(scanSpec)
    }

    override def scan(segmentNum: Int,
                      projectionExpression: String): ItemCollection[ScanOutcome] = {
        val scanSpec = getBaseScanSpec.withSegment(segmentNum).withProjectionExpression(projectionExpression)
        getClient.getTable(tableName).scan(scanSpec)
    }

    override def scan(segmentNum: Int,
                      projectionExpression: String,
                      filterExpression: String): ItemCollection[ScanOutcome] = {
        val scanSpec = getBaseScanSpec.withSegment(segmentNum)
            .withProjectionExpression(projectionExpression)
            .withFilterExpression(filterExpression)
        getClient.getTable(tableName).scan(scanSpec)
    }

    private def getBaseScanSpec: ScanSpec = new ScanSpec()
        .withTotalSegments(totalSegments)
        .withMaxPageSize(itemLimit)
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .withConsistentRead(consistentRead)

}
