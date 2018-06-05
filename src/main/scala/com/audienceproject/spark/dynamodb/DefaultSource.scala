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

import com.audienceproject.spark.dynamodb.rdd.{DynamoRelation, DynamoWriteRelation}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
        createRelation(sqlContext, parameters, null)
    }

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                                schema: StructType): BaseRelation = {
        new DynamoRelation(schema, parameters)(sqlContext)
    }

    override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
                                data: DataFrame): BaseRelation = {
        val writeRelation = new DynamoWriteRelation(data, parameters)(sqlContext)
        writeRelation.write()
        writeRelation
    }

}
