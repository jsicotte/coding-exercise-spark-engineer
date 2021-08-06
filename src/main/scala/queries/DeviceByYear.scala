package queries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace, year}

import java.util.Date

class DeviceByYear(var startDate: Date, var spark: SparkSession) {
  val dataFrame = spark.sql("select distinct RECEIPT_PURCHASE_DATE, CONSUMER_USER_AGENT from receipts")
    .withColumn("device", regexp_replace(col("CONSUMER_USER_AGENT"), "Fetch.*\\(", ""))
    .withColumn("device", regexp_replace(col("device"), ";.*", ""))
    .withColumn("year", year(col("RECEIPT_PURCHASE_DATE")))
    .groupBy("year", "device").count()
    .orderBy(col("count").desc)
}

object DeviceByYear {
  val fileName = "device_by_year"
}