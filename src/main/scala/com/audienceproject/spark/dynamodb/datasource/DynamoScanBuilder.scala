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

import com.audienceproject.spark.dynamodb.connector.{DynamoConnector, FilterPushdown}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

class DynamoScanBuilder(connector: DynamoConnector, schema: StructType)
    extends ScanBuilder
        with SupportsPushDownRequiredColumns
        with SupportsPushDownFilters {

    private var acceptedFilters: Array[Filter] = Array.empty
    private var currentSchema: StructType = schema

    override def build(): Scan = new DynamoBatchReader(connector, pushedFilters(), currentSchema)

    override def pruneColumns(requiredSchema: StructType): Unit = {
        val keyFields = Seq(Some(connector.keySchema.hashKeyName), connector.keySchema.rangeKeyName).flatten
            .flatMap(keyName => currentSchema.fields.find(_.name == keyName))
        val requiredFields = keyFields ++ requiredSchema.fields
        val newFields = currentSchema.fields.filter(requiredFields.contains)
        currentSchema = StructType(newFields)
    }

    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
        if (connector.filterPushdownEnabled) {
            val (acceptedFilters, postScanFilters) = FilterPushdown.acceptFilters(filters)
            this.acceptedFilters = acceptedFilters
            postScanFilters // Return filters that need to be evaluated after scanning.
        } else filters
    }

    override def pushedFilters(): Array[Filter] = acceptedFilters

}
