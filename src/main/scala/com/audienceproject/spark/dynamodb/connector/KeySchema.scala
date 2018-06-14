package com.audienceproject.spark.dynamodb.connector

import com.amazonaws.services.dynamodbv2.model.{KeySchemaElement, KeyType}

private[dynamodb] case class KeySchema(hashKeyName: String, rangeKeyName: Option[String])

private[dynamodb] object KeySchema {

    def fromDescription(keySchemaElements: Seq[KeySchemaElement]): KeySchema = {
        val hashKeyName = keySchemaElements.find(_.getKeyType == KeyType.HASH.toString).get.getAttributeName
        val rangeKeyName = keySchemaElements.find(_.getKeyType == KeyType.RANGE.toString).map(_.getAttributeName)
        KeySchema(hashKeyName, rangeKeyName)
    }

}
