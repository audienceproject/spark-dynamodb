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

import java.util

import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec.{BatchWriteItemSpec, ScanSpec, UpdateItemSpec}
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.{BOOL => newBOOL, L => newL, M => newM, N => newN, S => newS}
import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

private[dynamodb] class TableConnector(tableName: String, totalSegments: Int, parameters: Map[String, String])
    extends DynamoConnector with DynamoWritable with DynamoUpdatable with Serializable {

    private val consistentRead = parameters.getOrElse("stronglyConsistentReads", "false").toBoolean
    private val filterPushdown = parameters.getOrElse("filterPushdown", "true").toBoolean
    private val region = parameters.get("region")

    override val (keySchema, readLimit, writeLimit, itemLimit, totalSizeInBytes) = {
        val table = getDynamoDB(region).getTable(tableName)
        val desc = table.describe()

        // Key schema.
        val keySchema = KeySchema.fromDescription(desc.getKeySchema.asScala)

        // Parameters.
        val bytesPerRCU = parameters.getOrElse("bytesPerRCU", "4000").toInt
        val targetCapacity = parameters.getOrElse("targetCapacity", "1").toDouble
        val readFactor = if (consistentRead) 1 else 2

        // Provisioned or on-demand throughput.
        val readThroughput = Option(desc.getProvisionedThroughput.getReadCapacityUnits)
            .filter(_ > 0).map(_.longValue())
            .getOrElse(100L)
        val writeThroughput = Option(desc.getProvisionedThroughput.getWriteCapacityUnits)
            .filter(_ > 0).map(_.longValue())
            .getOrElse(100L)

        // Rate limit calculation.
        val tableSize = desc.getTableSizeBytes
        val avgItemSize = tableSize.toDouble / desc.getItemCount
        val readCapacity = readThroughput * targetCapacity
        val writeCapacity = writeThroughput * targetCapacity

        val readLimit = readCapacity / totalSegments
        val itemLimit = ((bytesPerRCU / avgItemSize * readLimit).toInt * readFactor) max 1

        val writeLimit = writeCapacity / totalSegments

        (keySchema, readLimit, writeLimit, itemLimit, tableSize.toLong)
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

        getDynamoDB(region).getTable(tableName).scan(scanSpec)
    }

    override def putItems(schema: StructType, batchSize: Int)(items: Iterator[Row]): Unit = {
        val columnNames = schema.map(_.name)
        val hashKeyIndex = columnNames.indexOf(keySchema.hashKeyName)
        val rangeKeyIndex = keySchema.rangeKeyName.map(columnNames.indexOf)
        val columnIndices = columnNames.zipWithIndex.filterNot({
            case (name, _) => keySchema match {
                case KeySchema(hashKey, None) => name == hashKey
                case KeySchema(hashKey, Some(rangeKey)) => name == hashKey || name == rangeKey
            }
        })

        val rateLimiter = RateLimiter.create(writeLimit max 1)
        val client = getDynamoDB(region)

        // For each batch.
        items.grouped(batchSize).foreach(itemBatch => {
            val batchWriteItemSpec = new BatchWriteItemSpec().withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            batchWriteItemSpec.withTableWriteItems(new TableWriteItems(tableName).withItemsToPut(
                // Map the items.
                itemBatch.map(row => {
                    val item = new Item()

                    // Map primary key.
                    keySchema match {
                        case KeySchema(hashKey, None) => item.withPrimaryKey(hashKey, row(hashKeyIndex))
                        case KeySchema(hashKey, Some(rangeKey)) =>
                            item.withPrimaryKey(hashKey, row(hashKeyIndex), rangeKey, row(rangeKeyIndex.get))
                    }

                    // Map remaining columns.
                    columnIndices.foreach({
                        case (name, index) if !row.isNullAt(index) =>
                            item.`with`(name, mapValue(row(index), schema(name).dataType))
                        case _ =>
                    })

                    item
                }): _*
            ))

            val response = client.batchWriteItem(batchWriteItemSpec)
            handleBatchWriteResponse(client, rateLimiter)(response)
        })
    }

    override def updateItems(schema: StructType)(items: Iterator[Row]): Unit = {
        val columnNames = schema.map(_.name)
        val hashKeyIndex = columnNames.indexOf(keySchema.hashKeyName)
        val rangeKeyIndex = keySchema.rangeKeyName.map(columnNames.indexOf)
        val columnIndices = columnNames.zipWithIndex.filterNot({
            case (name, _) => keySchema match {
                case KeySchema(hashKey, None) => name == hashKey
                case KeySchema(hashKey, Some(rangeKey)) => name == hashKey || name == rangeKey
            }
        })

        val rateLimiter = RateLimiter.create(writeLimit max 1)
        val client = getDynamoDB(region)

        // For each item.
        items.foreach(row => {
            // Build update expression.
            val xspec = new ExpressionSpecBuilder()
            columnIndices.foreach({
                case (name, index) if !row.isNullAt(index) =>
                    val updateAction = schema(name).dataType match {
                        case StringType => newS(name).set(row.getString(index))
                        case BooleanType => newBOOL(name).set(row.getBoolean(index))
                        case IntegerType => newN(name).set(row.getInt(index))
                        case LongType => newN(name).set(row.getLong(index))
                        case ShortType => newN(name).set(row.getShort(index))
                        case FloatType => newN(name).set(row.getFloat(index))
                        case DoubleType => newN(name).set(row.getDouble(index))
                        case ArrayType(innerType, _) => newL(name).set(row.getSeq[Any](index).map(e => mapValue(e, innerType)).asJava)
                        case MapType(keyType, valueType, _) =>
                            if (keyType != StringType) throw new IllegalArgumentException(
                                s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports String as Map key type.")
                            newM(name).set(row.getMap[String, Any](index).mapValues(e => mapValue(e, valueType)).asJava)
                        case StructType(fields) => newM(name).set(mapStruct(row.getStruct(index), fields))
                    }
                    xspec.addUpdate(updateAction)
                case _ =>
            })

            val updateItemSpec = new UpdateItemSpec()
                .withExpressionSpec(xspec.buildForUpdate())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

            // Map primary key.
            keySchema match {
                case KeySchema(hashKey, None) => updateItemSpec.withPrimaryKey(hashKey, row(hashKeyIndex))
                case KeySchema(hashKey, Some(rangeKey)) =>
                    updateItemSpec.withPrimaryKey(hashKey, row(hashKeyIndex), rangeKey, row(rangeKeyIndex.get))
            }

            if (updateItemSpec.getUpdateExpression.nonEmpty) {
                val response = client.getTable(tableName).updateItem(updateItemSpec)
                handleUpdateResponse(rateLimiter)(response)
            }
        })
    }

    private def mapValue(element: Any, elementType: DataType): Any = {
        elementType match {
            case ArrayType(innerType, _) => element.asInstanceOf[Seq[_]].map(e => mapValue(e, innerType)).asJava
            case MapType(keyType, valueType, _) =>
                if (keyType != StringType) throw new IllegalArgumentException(
                    s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports String as Map key type.")
                element.asInstanceOf[Map[String, _]].mapValues(e => mapValue(e, valueType)).asJava
            case StructType(fields) =>
                val row = element.asInstanceOf[Row]
                mapStruct(row, fields)
            case _ => element
        }
    }

    private def mapStruct(row: Row, fields: Seq[StructField]): util.Map[String, Any] =
        (fields.indices map { i =>
            fields(i).name -> mapValue(row(i), fields(i).dataType)
        }).toMap.asJava

    @tailrec
    private def handleBatchWriteResponse(client: DynamoDB, rateLimiter: RateLimiter)
                                        (response: BatchWriteItemOutcome): Unit = {
        // Rate limit on write capacity.
        if (response.getBatchWriteItemResult.getConsumedCapacity != null) {
            response.getBatchWriteItemResult.getConsumedCapacity.asScala.map(cap => {
                cap.getTableName -> cap.getCapacityUnits.toInt
            }).toMap.get(tableName).foreach(rateLimiter.acquire)
        }
        // Retry unprocessed items.
        if (response.getUnprocessedItems != null && !response.getUnprocessedItems.isEmpty) {
            val newResponse = client.batchWriteItemUnprocessed(response.getUnprocessedItems)
            handleBatchWriteResponse(client, rateLimiter)(newResponse)
        }
    }

    private def handleUpdateResponse(rateLimiter: RateLimiter)
                                    (response: UpdateItemOutcome): Unit = {
        // Rate limit on write capacity.
        Option(response.getUpdateItemResult.getConsumedCapacity)
            .map(_.getCapacityUnits.toInt)
            .foreach(rateLimiter.acquire)
    }

}
