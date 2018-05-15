package com.audienceproject.spark.dynamodb

import com.audienceproject.spark.dynamodb.implicits._

class FilterPushdownTest extends AbstractInMemoryTest {

    test("Count of red fruit is 2 (`EqualTo` filter)") {
        import spark.implicits._
        val fruitCount = spark.read.dynamodb("TestFruit").where($"color" === "red").count()
        assert(fruitCount === 2)
    }

    test("Count of yellow and green fruit is 4 (`In` filter)") {
        import spark.implicits._
        val fruitCount = spark.read.dynamodb("TestFruit")
            .where($"color" isin("yellow", "green"))
            .count()
        assert(fruitCount === 4)
    }

    test("Count of 0.01 weight fruit is 4 (`In` filter)") {
        import spark.implicits._
        val fruitCount = spark.read.dynamodb("TestFruit")
            .where($"weightKg" isin 0.01)
            .count()
        assert(fruitCount === 3)
    }

    test("Only 'banana' starts with a 'b' and is >0.01 kg (`StringStartsWith`, `GreaterThan`, `And` filters)") {
        import spark.implicits._
        val fruit = spark.read.dynamodb("TestFruit")
            .where(($"name" startsWith "b") && ($"weightKg" > 0.01))
            .collectAsList().get(0)
        assert(fruit.getAs[String]("name") === "banana")
    }

}
