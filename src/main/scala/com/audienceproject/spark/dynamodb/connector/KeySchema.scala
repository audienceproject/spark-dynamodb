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
package com.audienceproject.spark.dynamodb.connector

import com.amazonaws.services.dynamodbv2.model.{KeySchemaElement, KeyType}

private[dynamodb] case class KeySchema(hashKeyName: String, rangeKeyName: Option[String])

private[dynamodb] object KeySchema {

    def fromDescription(keySchemaElements: Seq[KeySchemaElement]): KeySchema = {
        val hashKeyName = keySchemaElements.find(_.getKeyType == KeyType.HASH.toString).get.getAttributeName
        val rangeKeyName = keySchemaElements.find(_.getKeyType == KeyType.RANGE.toString).map(_.getAttributeName)
        KeySchema(hashKeyName, rangeKeyName)
    }

}
