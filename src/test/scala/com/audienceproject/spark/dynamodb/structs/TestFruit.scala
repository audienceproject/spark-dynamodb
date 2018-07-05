package com.audienceproject.spark.dynamodb.structs

import com.audienceproject.spark.dynamodb.attribute

case class TestFruit(@attribute("name") primaryKey: String,
                     color: String,
                     weightKg: Double)
