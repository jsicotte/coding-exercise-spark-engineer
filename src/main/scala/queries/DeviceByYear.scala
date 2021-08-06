package queries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, regexp_replace, year}

import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

class DeviceByYear(var startInstant: Instant, var spark: SparkSession) {
  val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
  val startTimestamp = formatter.format(startInstant)

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