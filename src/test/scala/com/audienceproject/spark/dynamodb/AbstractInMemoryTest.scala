package com.audienceproject.spark.dynamodb

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item}
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, CreateTableRequest, KeySchemaElement, ProvisionedThroughput}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class AbstractInMemoryTest extends FunSuite with BeforeAndAfterAll {

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

        // Create a test table.
        dynamoDB.createTable(new CreateTableRequest()
            .withTableName("TestFruit")
            .withAttributeDefinitions(new AttributeDefinition("name", "S"))
            .withKeySchema(new KeySchemaElement("name", "HASH"))
            .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)))

        // Populate with test data.
        val table = dynamoDB.getTable("TestFruit")
        for ((name, color, weight) <- Seq(
            ("apple", "red", 0.2), ("banana", "yellow", 0.15), ("watermelon", "red", 0.5),
            ("grape", "green", 0.01), ("pear", "green", 0.2), ("kiwi", "green", 0.05),
            ("blackberry", "purple", 0.01), ("blueberry", "purple", 0.01), ("plum", "purple", 0.1))
        ) {
            table.putItem(new Item()
                .withString("name", name)
                .withString("color", color)
                .withDouble("weightKg", weight))
        }
    }

    override def afterAll(): Unit = {
        server.stop()
    }

}
