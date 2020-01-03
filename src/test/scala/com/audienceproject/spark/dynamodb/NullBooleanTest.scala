package com.audienceproject.spark.dynamodb

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model.{
    AttributeDefinition,
    CreateTableRequest,
    KeySchemaElement,
    ProvisionedThroughput
}
import com.audienceproject.spark.dynamodb.implicits._

class NullBooleanTest extends AbstractInMemoryTest {
    test("Test Null") {
        dynamoDB.createTable(
            new CreateTableRequest()
                .withTableName("TestNullBoolean")
                .withAttributeDefinitions(new AttributeDefinition("Pk", "S"))
                .withKeySchema(new KeySchemaElement("Pk", "HASH"))
                .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L))
        )

        val table = dynamoDB.getTable("TestNullBoolean")

        for ((_pk, _type, _value) <- Seq(
            ("id1", "type1", true),
            ("id2", "type2", null)
        )) {
            if (_type != "type2") {
                table.putItem(
                    new Item()
                        .withString("Pk", _pk)
                        .withString("Type", _type)
                        .withBoolean("Value", _value.asInstanceOf[Boolean])
                )
            } else {
                table.putItem(
                    new Item()
                        .withString("Pk", _pk)
                        .withString("Type", _type)
                        .withNull("Value")
                )
            }
        }

        val df = spark.read.dynamodbAs[BooleanClass]("TestNullBoolean")

        import spark.implicits._
        df.where($"Type" === "type2").show()
        client.deleteTable("TestNullBoolean")
    }
}

case class BooleanClass(Pk: String, Type: String, Value: Boolean)
