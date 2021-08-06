package queries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, regexp_replace, year}

import java.time.Instant

class DeviceByYear(startInstant: Instant, spark: SparkSession) extends BaseQuery(startInstant, spark) {
  val dataFrame = spark.sql("select distinct RECEIPT_PURCHASE_DATE, CONSUMER_USER_AGENT from receipts")
    .filter(col("RECEIPT_PURCHASE_DATE" ).gt(lit(startTimestamp)))
    .withColumn("device", regexp_replace(col("CONSUMER_USER_AGENT"), "Fetch.*\\(", ""))
    .withColumn("device", regexp_replace(col("device"), ";.*", ""))
    .withColumn("year", year(col("RECEIPT_PURCHASE_DATE")))
    .groupBy("year", "device").count()
    .orderBy(col("count").desc)
}

object DeviceByYear {
  val fileName = "device_by_year"
}