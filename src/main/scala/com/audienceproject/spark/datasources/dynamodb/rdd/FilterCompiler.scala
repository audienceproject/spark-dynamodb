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

import org.apache.spark.sql.sources._

private[dynamodb] object FilterCompiler {

    def apply(filters: Seq[Filter]): String = filters.map(compile).map("(" + _ + ")").mkString(" AND ")

    private def compile(filter: Filter): String = filter match {
        case EqualTo(attribute, value) => s"$attribute = $value"
        case GreaterThan(attribute, value) => s"$attribute > $value"
        case GreaterThanOrEqual(attribute, value) => s"$attribute >= $value"
        case LessThan(attribute, value) => s"$attribute < $value"
        case LessThanOrEqual(attribute, value) => s"$attribute <= $value"
        case In(attribute, values) => s"$attribute IN (${values.mkString(",")})"
        case IsNull(attribute) => s"attribute_exists($attribute)"
        case IsNotNull(attribute) => s"attribute_not_exists($attribute)"
        case StringStartsWith(attribute, value) => s"begins_with($attribute, $value)"
        case StringContains(attribute, value) => s"contains($attribute, $value)"
        case StringEndsWith(_, _) => throw new UnsupportedOperationException("Filter `StringEndsWith` is not supported by DynamoDB")
        case And(left, right) => s"(${compile(left)}) AND (${compile(right)})"
        case Or(left, right) => s"(${compile(left)}) OR (${compile(right)})"
        case Not(f) => s"NOT (${compile(f)})"
    }

}
