package queries

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

/**
 * Base class for queries. Since we want to support a start date for each query, this has been abstracted here.
 * All time is assumed to be in UTC.
 *
 * @param startInstant the beginning of the date window
 * @param spark
 */
abstract class BaseQuery(var startInstant: Instant, var spark: SparkSession) {
  val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
  val startTimestamp = formatter.format(startInstant)
  val dataFrame: DataFrame
}
