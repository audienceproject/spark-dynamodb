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
package com.audienceproject.spark.datasources.dynamodb.rdd

import com.amazonaws.services.dynamodbv2.document.Item
import org.apache.spark.sql.types._

private[dynamodb] object TypeConversion {

    def apply(attrName: String, sparkType: DataType): Item => Any =
        (sparkType match {
            case StringType => nullableGet(_.getString) _
            case IntegerType => nullableGet(_.getInt) _
            case LongType => nullableGet(_.getLong) _
            case DoubleType => nullableGet(_.getDouble) _
            case FloatType => nullableGet(_.getFloat) _
            case _ => throw new IllegalArgumentException(s"Spark DataType '${sparkType.typeName}' could not be mapped to a corresponding DynamoDB data type.")
        }) (attrName)

    private def nullableGet(getter: Item => String => Any)(attrName: String): Item => Any = item =>
        if (item.hasAttribute(attrName)) getter(item)(attrName)
        else null

}
