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

import com.audienceproject.spark.dynamodb.connector.DynamoConnector
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class DynamoBatchReader(connector: DynamoConnector,
                        filters: Array[Filter],
                        schema: StructType)
    extends Scan with Batch with SupportsReportPartitioning {

    override def readSchema(): StructType = schema

    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = {
        val requiredColumns = schema.map(_.name)
        Array.tabulate(connector.totalSegments)(new ScanPartition(_, requiredColumns, filters))
    }

    override def createReaderFactory(): PartitionReaderFactory =
        new DynamoReaderFactory(connector, schema)

    override val outputPartitioning: Partitioning = new OutputPartitioning(connector.totalSegments)

}
