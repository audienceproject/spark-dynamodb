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

import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec.{BatchWriteItemSpec, ScanSpec}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ReturnConsumedCapacity, UpdateItemRequest}
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import com.audienceproject.spark.dynamodb.catalyst.JavaConverter
import com.audienceproject.spark.dynamodb.util.RateLimiter
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

private[dynamodb] class TableConnector(tableName: String, parallelism: Int, parameters: Map[String, String])
    extends DynamoConnector with DynamoWritable with DynamoUpdatable with Serializable {

    private val consistentRead = parameters.getOrElse("stronglyconsistentreads", "false").toBoolean
    private val filterPushdown = parameters.getOrElse("filterpushdown", "true").toBoolean
    private val region = parameters.get("region")
    private val roleArn = parameters.get("rolearn")

    override val filterPushdownEnabled: Boolean = filterPushdown

    override val (keySchema, readLimit, writeLimit, itemLimit, totalSegments) = {
        val table = getDynamoDB(region, roleArn).getTable(tableName)
        val desc = table.describe()

        // Key schema.
        val keySchema = KeySchema.fromDescription(desc.getKeySchema.asScala)

        // User parameters.
        val bytesPerRCU = parameters.getOrElse("bytesperrcu", "4000").toInt
        val maxPartitionBytes = parameters.getOrElse("maxpartitionbytes", "128000000").toInt
        val targetCapacity = parameters.getOrElse("targetcapacity", "1").toDouble
        val readFactor = if (consistentRead) 1 else 2

        // Table parameters.
        val tableSize = desc.getTableSizeBytes
        val itemCount = desc.getItemCount

        // Partitioning calculation.
        val numPartitions = parameters.get("readpartitions").map(_.toInt).getOrElse(
            (tableSize / maxPartitionBytes).toInt max 1
        )

        // Provisioned or on-demand throughput.
        val readThroughput = parameters.getOrElse("throughput", Option(desc.getProvisionedThroughput.getReadCapacityUnits)
            .filter(_ > 0).map(_.longValue().toString)
            .getOrElse("100")).toLong
        val writeThroughput = parameters.getOrElse("throughput", Option(desc.getProvisionedThroughput.getWriteCapacityUnits)
            .filter(_ > 0).map(_.longValue().toString)
            .getOrElse("100")).toLong

        // Rate limit calculation.
        val avgItemSize = tableSize.toDouble / itemCount
        val readCapacity = readThroughput * targetCapacity
        val writeCapacity = writeThroughput * targetCapacity

        val readLimit = readCapacity / parallelism
        val itemLimit = ((bytesPerRCU / avgItemSize * readLimit).toInt * readFactor) max 1

        val writeLimit = writeCapacity / parallelism

        (keySchema, readLimit, writeLimit, itemLimit, numPartitions)
    }

    override def scan(segmentNum: Int, columns: Seq[String], filters: Seq[Filter]): ItemCollection[ScanOutcome] = {
        val scanSpec = new ScanSpec()
            .withSegment(segmentNum)
            .withTotalSegments(totalSegments)
            .withMaxPageSize(itemLimit)
            .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            .withConsistentRead(consistentRead)

        if (columns.nonEmpty) {
            val xspec = new ExpressionSpecBuilder().addProjections(columns: _*)

            if (filters.nonEmpty && filterPushdown) {
                xspec.withCondition(FilterPushdown(filters))
            }

            scanSpec.withExpressionSpec(xspec.buildForScan())
        }

        getDynamoDB(region, roleArn).getTable(tableName).scan(scanSpec)
    }

    override def putItems(schema: StructType, items: Seq[InternalRow]): Unit = {
        val columnNames = schema.map(_.name)
        val hashKeyIndex = columnNames.indexOf(keySchema.hashKeyName)
        val rangeKeyIndex = keySchema.rangeKeyName.map(columnNames.indexOf)
        val columnIndices = columnNames.zipWithIndex.filterNot({
            case (name, _) => keySchema match {
                case KeySchema(hashKey, None) => name == hashKey
                case KeySchema(hashKey, Some(rangeKey)) => name == hashKey || name == rangeKey
            }
        })

        val rateLimiter = new RateLimiter(writeLimit)
        val client = getDynamoDB(region, roleArn)

        // For each batch.
        val batchWriteItemSpec = new BatchWriteItemSpec().withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        batchWriteItemSpec.withTableWriteItems(new TableWriteItems(tableName).withItemsToPut(
            // Map the items.
            items.map(row => {
                val item = new Item()

                // Map primary key.
                keySchema match {
                    case KeySchema(hashKey, None) =>
                        val hashKeyType = schema(hashKey).dataType
                        item.withPrimaryKey(hashKey, JavaConverter.extractRowValue(row, hashKeyIndex, hashKeyType))
                    case KeySchema(hashKey, Some(rangeKey)) =>
                        val hashKeyValue = JavaConverter.extractRowValue(row, hashKeyIndex, schema(hashKey).dataType)
                        val rangeKeyValue = JavaConverter.extractRowValue(row, rangeKeyIndex.get, schema(rangeKey).dataType)
                        item.withPrimaryKey(hashKey, hashKeyValue, rangeKey, rangeKeyValue)
                }

                // Map remaining columns.
                columnIndices.foreach({
                    case (name, index) if !row.isNullAt(index) =>
                        item.`with`(name, JavaConverter.extractRowValue(row, index, schema(name).dataType))
                    case _ =>
                })

                item
            }): _*
        ))

        val response = client.batchWriteItem(batchWriteItemSpec)
        handleBatchWriteResponse(client, rateLimiter)(response)
    }

    override def updateItems(schema: StructType, batchSize: Int)(items: Iterator[Row]): Unit = {
        val columnNames = schema.map(_.name)
        val hashKeyIndex = columnNames.indexOf(keySchema.hashKeyName)
        val rangeKeyIndex = keySchema.rangeKeyName.map(columnNames.indexOf)
        val columnIndices = columnNames.zipWithIndex.filterNot({
            case (name, _) => keySchema match {
                case KeySchema(hashKey, None) => name == hashKey
                case KeySchema(hashKey, Some(rangeKey)) => name == hashKey || name == rangeKey
            }
        })

        //val rateLimiter = new RateLimiter(writeLimit) // TODO: Make thread safe. Why async in the first place?
        val client = getDynamoDBAsyncClient(region, roleArn)

        // For each item.
        items.grouped(batchSize).foreach(itemBatch => {
            itemBatch.map(row => {
                val key: Map[String, AttributeValue] = keySchema match {
                    case KeySchema(hashKey, None) => Map(hashKey -> mapValueToAttributeValue(row(hashKeyIndex), schema(hashKey).dataType))
                    case KeySchema(hashKey, Some(rangeKey)) =>
                        Map(hashKey -> mapValueToAttributeValue(row(hashKeyIndex), schema(hashKey).dataType),
                            rangeKey -> mapValueToAttributeValue(row(rangeKeyIndex.get), schema(rangeKey).dataType))

                }
                val nonNullColumnIndices = columnIndices.filter(c => row(c._2) != null)
                val updateExpression = s"SET ${nonNullColumnIndices.map(c => s"#${c._2}=:${c._2}").mkString(", ")}" // TODO: Use xspec.
                val expressionAttributeValues = nonNullColumnIndices.map(c => s":${c._2}" -> mapValueToAttributeValue(row(c._2), schema(c._1).dataType)).toMap.asJava
                val updateItemReq = new UpdateItemRequest()
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    .withTableName(tableName)
                    .withKey(key.asJava)
                    .withUpdateExpression(updateExpression)
                    .withExpressionAttributeNames(nonNullColumnIndices.map(c => s"#${c._2}" -> c._1).toMap.asJava)
                    .withExpressionAttributeValues(expressionAttributeValues)

                client.updateItemAsync(updateItemReq)
            })
        })
    }

    private def mapValueToAttributeValue(element: Any, elementType: DataType): AttributeValue = {
        elementType match {
            case ArrayType(innerType, _) => new AttributeValue().withL(element.asInstanceOf[Seq[_]].map(e => mapValueToAttributeValue(e, innerType)): _*)
            case MapType(keyType, valueType, _) =>
                if (keyType != StringType) throw new IllegalArgumentException(
                    s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports String as Map key type.")

                new AttributeValue().withM(element.asInstanceOf[Map[String, _]].mapValues(e => mapValueToAttributeValue(e, valueType)).asJava)

            case StructType(fields) =>
                val row = element.asInstanceOf[Row]
                new AttributeValue().withM((fields.indices map { i =>
                    fields(i).name -> mapValueToAttributeValue(row(i), fields(i).dataType)
                }).toMap.asJava)
            case StringType => new AttributeValue().withS(element.asInstanceOf[String])
            case LongType | IntegerType | DoubleType | FloatType => new AttributeValue().withN(element.toString)
            case BooleanType => new AttributeValue().withBOOL(element.asInstanceOf[Boolean])
        }
    }

    @tailrec
    private def handleBatchWriteResponse(client: DynamoDB, rateLimiter: RateLimiter)
                                        (response: BatchWriteItemOutcome): Unit = {
        // Rate limit on write capacity.
        if (response.getBatchWriteItemResult.getConsumedCapacity != null) {
            response.getBatchWriteItemResult.getConsumedCapacity.asScala.map(cap => {
                cap.getTableName -> cap.getCapacityUnits.toInt
            }).toMap.get(tableName).foreach(units => rateLimiter.acquire(units))
        }
        // Retry unprocessed items.
        if (response.getUnprocessedItems != null && !response.getUnprocessedItems.isEmpty) {
            val newResponse = client.batchWriteItemUnprocessed(response.getUnprocessedItems)
            handleBatchWriteResponse(client, rateLimiter)(newResponse)
        }
    }

}
