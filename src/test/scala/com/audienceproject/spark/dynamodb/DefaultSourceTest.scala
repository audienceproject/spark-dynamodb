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

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item}
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, CreateTableRequest, KeySchemaElement, ProvisionedThroughput}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.audienceproject.spark.dynamodb.implicits._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DefaultSourceTest extends FunSuite with BeforeAndAfterAll {

    val server: DynamoDBProxyServer = ServerRunner.createServerFromCommandLineArgs(Array("-inMemory"))

    val client: AmazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
        .withEndpointConfiguration(new EndpointConfiguration(System.getProperty("aws.dynamodb.endpoint"), "us-east-1"))
        .build()
    val dynamoDB: DynamoDB = new DynamoDB(client)

    val spark: SparkSession = SparkSession.builder
        .master("local")
        .appName(this.getClass.getName)
        .getOrCreate()

    override def beforeAll(): Unit = {
        server.start()
    }

    override def afterAll(): Unit = {
        server.stop()
    }

    test("The apple is red") {
        dynamoDB.createTable(new CreateTableRequest()
            .withTableName("TestVegetables")
            .withAttributeDefinitions(new AttributeDefinition("name", "S"))
            .withKeySchema(new KeySchemaElement("name", "HASH"))
            .withProvisionedThroughput(new ProvisionedThroughput(100L, 5L)))

        val table = dynamoDB.getTable("TestVegetables")
        table.putItem(new Item().withString("name", "Apple").withString("color", "red"))

        assert(table.getItem("name", "Apple").getString("color") === "red")

        val count = spark.read.dynamodb("TestVegetables").count()

        assert(count === 1)
    }

}
