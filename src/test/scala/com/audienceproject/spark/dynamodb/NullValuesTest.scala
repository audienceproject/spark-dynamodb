package com.audienceproject.spark.dynamodb

import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, CreateTableRequest, KeySchemaElement, ProvisionedThroughput}
import com.audienceproject.spark.dynamodb.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class NullValuesTest extends AbstractInMemoryTest {

    test("Insert nested StructType with null values") {
        dynamoDB.createTable(new CreateTableRequest()
            .withTableName("NullTest")
            .withAttributeDefinitions(new AttributeDefinition("name", "S"))
            .withKeySchema(new KeySchemaElement("name", "HASH"))
            .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)))

        val schema = StructType(
            Seq(
                StructField("name", StringType, nullable = false),
                StructField("info", StructType(
                    Seq(
                        StructField("age", IntegerType, nullable = true),
                        StructField("address", StringType, nullable = true)
                    )
                ), nullable = true)
            )
        )

        val rows = spark.sparkContext.parallelize(Seq(
            Row("one", Row(30, "Somewhere")),
            Row("two", null),
            Row("three", Row(null, null))
        ))

        val newItemsDs = spark.createDataFrame(rows, schema)

        newItemsDs.write.dynamodb("NullTest")

        val validationDs = spark.read.dynamodb("NullTest")

        validationDs.show(false)
    }

}
