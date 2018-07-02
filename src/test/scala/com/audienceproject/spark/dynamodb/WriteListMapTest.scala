package com.audienceproject.spark.dynamodb

import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, CreateTableRequest, KeySchemaElement, ProvisionedThroughput}
import com.audienceproject.spark.dynamodb.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class WriteListMapTest extends AbstractInMemoryTest {

    test("Inserting from a local Dataset") {
        dynamoDB.createTable(new CreateTableRequest()
            .withTableName("InsertTestListMap")
            .withAttributeDefinitions(new AttributeDefinition("name", "S"))
            .withKeySchema(new KeySchemaElement("name", "HASH"))
            .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)))

        import spark.implicits._

        val fruitSchema = StructType(
          Seq(
          StructField("name", StringType, nullable = false),
          StructField("color", StringType, nullable = false),
          StructField("weight", DoubleType, nullable = false),
          StructField("properties", ArrayType(MapType(StringType,StringType, valueContainsNull = false), containsNull = false), nullable = false)
        ))

        val rows = spark.sparkContext.parallelize(Seq(
          Row("lemon", "yellow", 0.1, Seq(Map("freshness" -> "fresh", "eco" -> "yes", "price" -> "2 dkk" ))),
          Row("orange", "orange", 0.2, Seq(Map("freshness" -> "too ripe", "eco" -> "no", "price" -> "1 dkk" ))),
          Row("pomegranate", "red", 0.2, Seq(Map("freshness" -> "green", "eco" -> "yes", "price" -> "4 dkk" )))
            ))

        val newItemsDs = spark.createDataFrame(rows, fruitSchema)

        newItemsDs.show(false)

        newItemsDs.write.dynamodb("InsertTestListMap")

        println("Writing successful.")

        val validationDs = spark.read.dynamodb("InsertTestListMap")
        validationDs.show(false)
        assert(validationDs.count() === 3)
        assert(validationDs.select("name").as[String].collect().forall(Seq("lemon", "orange", "pomegranate") contains _))
        assert(validationDs.select("color").as[String].collect().forall(Seq("yellow", "orange", "red") contains _))
        assert(validationDs.select("weight").as[Double].collect().forall(Seq(0.1, 0.2, 0.2) contains _))
    }

    test("Reading and inserting lists of maps on the same table") {
    }

}
