package com.audienceproject.spark.dynamodb.catalyst

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

object JavaConverter {

    def extractRowValue(row: InternalRow, index: Int, elementType: DataType): Any = {
        elementType match {
            case ArrayType(innerType, _) => extractArray(row.getArray(index), innerType)
            case MapType(keyType, valueType, _) => extractMap(row.getMap(index), keyType, valueType)
            case StructType(fields) => mapStruct(row.getStruct(index, fields.length), fields)
            case StringType => row.getString(index)
            case _ => row.get(index, elementType)
        }
    }

    def extractArray(array: ArrayData, elementType: DataType): Any = {
        elementType match {
            case ArrayType(innerType, _) => array.toSeq[ArrayData](elementType).map(extractArray(_, innerType)).asJava
            case MapType(keyType, valueType, _) => array.toSeq[MapData](elementType).map(extractMap(_, keyType, valueType)).asJava
            case structType: StructType => array.toSeq[InternalRow](structType).map(mapStruct(_, structType.fields)).asJava
            case StringType => convertStringArray(array).asJava
            case _ => array.toSeq[Any](elementType).asJava
        }
    }

    def extractMap(map: MapData, keyType: DataType, valueType: DataType): util.Map[String, Any] = {
        if (keyType != StringType) throw new IllegalArgumentException(
            s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports String as Map key type.")
        val keys = convertStringArray(map.keyArray())
        val values = valueType match {
            case ArrayType(innerType, _) => map.valueArray().toSeq[ArrayData](valueType).map(extractArray(_, innerType))
            case MapType(innerKeyType, innerValueType, _) => map.valueArray().toSeq[MapData](valueType).map(extractMap(_, innerKeyType, innerValueType))
            case structType: StructType => map.valueArray().toSeq[InternalRow](structType).map(mapStruct(_, structType.fields))
            case StringType => convertStringArray(map.valueArray())
            case _ => map.valueArray().toSeq[Any](valueType)
        }
        val kvPairs = for (i <- 0 until map.numElements()) yield keys(i) -> values(i)
        Map(kvPairs: _*).asJava
    }

    def mapStruct(row: InternalRow, fields: Seq[StructField]): util.Map[String, Any] = {
        val kvPairs = for (i <- 0 until row.numFields)
            yield fields(i).name -> extractRowValue(row, i, fields(i).dataType)
        Map(kvPairs: _*).asJava
    }


    def convertStringArray(array: ArrayData): Seq[String] =
        array.toSeq[UTF8String](StringType).map(_.toString)

}
