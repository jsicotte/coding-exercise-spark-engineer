package queries

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

abstract class BaseQuery(var startInstant: Instant, var spark: SparkSession) {
  val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
  val startTimestamp = formatter.format(startInstant)
  val dataFrame: DataFrame
}
