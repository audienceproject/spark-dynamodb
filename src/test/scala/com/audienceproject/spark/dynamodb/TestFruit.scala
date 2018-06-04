package com.audienceproject.spark.dynamodb

case class TestFruit(@attribute("name") primaryKey: String,
                     color: String,
                     weightKg: Double)
