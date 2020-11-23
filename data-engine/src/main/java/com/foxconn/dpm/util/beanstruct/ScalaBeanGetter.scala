package com.foxconn.dpm.util.beanstruct

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import scala.collection.JavaConverters._
object ScalaBeanGetter {
    def getRowStructFiles(row: Row) : java.util.List[StructField] = {
        row.schema.asJava
    }
}
