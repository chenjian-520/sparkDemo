package com.foxconn.dpm.dws_ads.batchData

import java.util
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructField
import scala.collection.JavaConverters._

object RowDataUtil {

    def replaceRowValueIsOrder(row: Row, values: Array[Object]): Row = {
        try {
            val orignData: List[Any] = Row.unapplySeq(row).get.toList
            for (i <- 0 until values.length){
                orignData.updated(i, values(i))
            }
            new GenericRowWithSchema(orignData.toArray, row.schema)
        } catch {
            case e: Exception => {
                null
            }
        }

    }

    def replaceRowValueIsMap(row: Row, values: java.util.HashMap[String, Object]): Row = {
        try {
            val orignData: List[Any] = Row.unapplySeq(row).get.toList
            val schema: util.List[StructField] = row.schema.asJava
            var i : Int = 0
            for (i <- 0 until row.schema.size){
                val v: Object = values.get(schema.get(i).name)
                if (v != null){
                    orignData.updated(i, v)
                }
            }
            new GenericRowWithSchema(orignData.toArray, row.schema)
        } catch {
            case e: Exception => {
                null
            }
        }
    }

}
