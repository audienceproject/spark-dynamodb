package com.audienceproject.spark.dynamodb

import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, CreateTableRequest, KeySchemaElement, ProvisionedThroughput}
import com.audienceproject.spark.dynamodb.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.struct

class WriteStructTest extends AbstractInMemoryTest {

    test("Inserting from a local Dataset") {
        dynamoDB.createTable(new CreateTableRequest()
            .withTableName("InsertTestStruct")
            .withAttributeDefinitions(new AttributeDefinition("name", "S"))
            .withKeySchema(new KeySchemaElement("name", "HASH"))
            .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)))

        import spark.implicits._

        val fruitSchema = StructType(
          Seq(
          StructField("name", StringType, nullable = false),
          StructField("color", StringType, nullable = false),
          StructField("weight", DoubleType, nullable = false),
          StructField("freshness", StringType, nullable = false),
          StructField("eco", BooleanType, nullable = false),
          StructField("price", DoubleType, nullable = false)
          ))

        val rows = spark.sparkContext.parallelize(Seq(
          Row("lemon", "yellow", 0.1,  "fresh",  true, 2.0d ),
          Row("pomegranate", "red", 0.2, "green", true, 4.0d )
            ))

        val newItemsDs = spark.createDataFrame(rows, fruitSchema).select(
          $"name",
          $"color",
          $"weight",
          struct($"freshness", $"eco",$"price" ) as "properties"
        )

        newItemsDs.printSchema()
        newItemsDs.show(false)

        newItemsDs.write.dynamodb("InsertTestStruct")

        println("Writing successful.")

        val validationDs = spark.read.dynamodb("InsertTestStruct")
        validationDs.show(false)
        assert(validationDs.count() === 3)
        assert(validationDs.select("name").as[String].collect().forall(Seq("lemon", "orange", "pomegranate") contains _))
        assert(validationDs.select("color").as[String].collect().forall(Seq("yellow", "orange", "red") contains _))
        assert(validationDs.select("weight").as[Double].collect().forall(Seq(0.1, 0.2, 0.2) contains _))
    }

    test("Reading and inserting lists of maps on the same table") {
    }

}
