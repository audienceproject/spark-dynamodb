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

import java.util

import collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, CreateTableRequest, KeySchemaElement, KeyType, ProvisionedThroughput}
import com.audienceproject.spark.dynamodb.implicits._
import org.apache.spark.sql.functions.{lit, when, length => sqlLength}
import org.scalatest.Matchers

class WriteRelationTest extends AbstractInMemoryTest with Matchers {

    test("Inserting from a local Dataset") {
        dynamoDB.createTable(new CreateTableRequest()
            .withTableName("InsertTest1")
            .withAttributeDefinitions(new AttributeDefinition("name", "S"))
            .withKeySchema(new KeySchemaElement("name", "HASH"))
            .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)))

        import spark.implicits._

        val newItemsDs = spark.createDataset(Seq(
            ("lemon", "yellow", 0.1),
            ("orange", "orange", 0.2),
            ("pomegranate", "red", 0.2)
        ))
            .withColumnRenamed("_1", "name")
            .withColumnRenamed("_2", "color")
            .withColumnRenamed("_3", "weight")
        newItemsDs.write.dynamodb("InsertTest1")

        val validationDs = spark.read.dynamodb("InsertTest1")
        assert(validationDs.count() === 3)
        assert(validationDs.select("name").as[String].collect().forall(Seq("lemon", "orange", "pomegranate") contains _))
        assert(validationDs.select("color").as[String].collect().forall(Seq("yellow", "orange", "red") contains _))
        assert(validationDs.select("weight").as[Double].collect().forall(Seq(0.1, 0.2, 0.2) contains _))
    }

    test("Deleting from a local Dataset with a HashKey only") {
        val tablename = "DeleteTest1"
        dynamoDB.createTable(new CreateTableRequest()
            .withTableName(tablename)
            .withAttributeDefinitions(new AttributeDefinition("name", "S"))
            .withKeySchema(new KeySchemaElement("name", "HASH"))
            .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)))

        import spark.implicits._

        val newItemsDs = Seq(
            ("lemon", "yellow", 0.1),
            ("orange", "orange", 0.2),
            ("pomegranate", "red", 0.2)
        ).toDF("name", "color", "weight")
        newItemsDs.write.dynamodb(tablename)

        val toDelete = Seq(
            ("lemon", "yellow"),
            ("orange", "blue"),
            ("doesn't exist", "black")
        ).toDF("name", "color")
        toDelete.write.option("delete", "true").dynamodb(tablename)

        val validationDs = spark.read.dynamodb(tablename)
        validationDs.count() shouldEqual 1
        val rec = validationDs.first
        rec.getString(rec.fieldIndex("name")) shouldEqual "pomegranate"
        rec.getString(rec.fieldIndex("color")) shouldEqual "red"
        rec.getDouble(rec.fieldIndex("weight")) shouldEqual 0.2
    }

    test("Deleting from a local Dataset with a HashKey and RangeKey") {
        val tablename = "DeleteTest2"

        dynamoDB.createTable(new CreateTableRequest()
            .withTableName(tablename)
            .withAttributeDefinitions(Seq(
                new AttributeDefinition("name", "S"),
                new AttributeDefinition("weight", "N")
            ).asJavaCollection)
            .withKeySchema(Seq(
                new KeySchemaElement("name", KeyType.HASH),
                // also test that non-string key works
                new KeySchemaElement("weight", KeyType.RANGE)
            ).asJavaCollection)
            .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)))

        import spark.implicits._

        val newItemsDs = Seq(
            ("lemon", "yellow", 0.1),
            ("lemon", "blue", 4.0),
            ("orange", "orange", 0.2),
            ("pomegranate", "red", 0.2)
        ).toDF("name", "color", "weight")
        newItemsDs.write.dynamodb(tablename)

        val toDelete = Seq(
            ("lemon", "yellow", 0.1),
            ("orange", "orange", 0.2),
            ("pomegranate", "shouldn'tdelete", 0.5)
        ).toDF("name", "color", "weight")
        toDelete.write.option("delete", "true").dynamodb(tablename)

        val validationDs = spark.read.dynamodb(tablename)
        validationDs.show
        validationDs.count() shouldEqual 2
        validationDs.select("name").as[String].collect should contain theSameElementsAs Seq("lemon", "pomegranate")
        validationDs.select("color").as[String].collect should contain theSameElementsAs Seq("blue", "red")
    }

    test("Updating from a local Dataset with new and only some previous columns") {
        val tablename = "UpdateTest1"
        dynamoDB.createTable(new CreateTableRequest()
            .withTableName(tablename)
            .withAttributeDefinitions(new AttributeDefinition("name", "S"))
            .withKeySchema(new KeySchemaElement("name", "HASH"))
            .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)))

        import spark.implicits._

        val newItemsDs = Seq(
            ("lemon", "yellow", 0.1),
            ("orange", "orange", 0.2),
            ("pomegranate", "red", 0.2)
        ).toDF("name", "color", "weight")
        newItemsDs.write.dynamodb(tablename)

        newItemsDs
            .withColumn("size", sqlLength($"color"))
            .drop("color")
            .withColumn("weight", $"weight" * 2)
            .write.option("update", "true").dynamodb(tablename)

        val validationDs = spark.read.dynamodb(tablename)
        validationDs.show
        assert(validationDs.count() === 3)
        assert(validationDs.select("name").as[String].collect().forall(Seq("lemon", "orange", "pomegranate") contains _))
        assert(validationDs.select("color").as[String].collect().forall(Seq("yellow", "orange", "red") contains _))
        assert(validationDs.select("weight").as[Double].collect().forall(Seq(0.2, 0.4, 0.4) contains _))
        assert(validationDs.select("size").as[Long].collect().forall(Seq(6, 3) contains _))
    }

    test("Updating from a local Dataset with null values") {
        val tablename = "UpdateTest2"
        dynamoDB.createTable(new CreateTableRequest()
            .withTableName(tablename)
            .withAttributeDefinitions(new AttributeDefinition("name", "S"))
            .withKeySchema(new KeySchemaElement("name", "HASH"))
            .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)))

        import spark.implicits._

        val newItemsDs = Seq(
            ("lemon", "yellow", 0.1),
            ("orange", "orange", 0.2),
            ("pomegranate", "red", 0.2)
        ).toDF("name", "color", "weight")
        newItemsDs.write.dynamodb(tablename)

        val alteredDs = newItemsDs
            .withColumn("weight", when($"weight" < 0.2, $"weight").otherwise(lit(null)))
        alteredDs.show
        alteredDs.write.option("update", "true").dynamodb(tablename)

        val validationDs = spark.read.dynamodb(tablename)
        validationDs.show
        assert(validationDs.count() === 3)
        assert(validationDs.select("name").as[String].collect().forall(Seq("lemon", "orange", "pomegranate") contains _))
        assert(validationDs.select("color").as[String].collect().forall(Seq("yellow", "orange", "red") contains _))
        assert(validationDs.select("weight").as[Double].collect().forall(Seq(0.2, 0.1) contains _))
    }

}
