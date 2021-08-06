import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}
import queries.{CategoryPopularity, DeviceByYear, TotalPurchaseByStore}
import scopt.OParser

import java.time.{ZoneOffset, ZonedDateTime}

object SparkEngineerChallenge {
  def createWriter(outputFormat: String, filename: String) = {
    if (outputFormat == "json") {
      (dataFrame: DataFrame) => dataFrame.write.json(filename)
    } else if (outputFormat == "csv") {
      (dataFrame: DataFrame) => dataFrame.write.csv(filename)
    } else {
      (dataFrame: DataFrame) => dataFrame.write.parquet(filename)
    }
  }

  def runReports(local: Boolean, outputFormat: String, report: String, numMonths: Int): Unit = {
    val spark = if (local) {
      SparkSession.builder.master("local[4]").appName("Spark Challenge").getOrCreate()
    } else {
      SparkSession.builder().getOrCreate()
    }

    val receipts = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .csv("rewards_receipts_lat_v3.csv")
      .withColumn("RECEIPT_PURCHASE_DATE", to_timestamp(col("RECEIPT_PURCHASE_DATE"), "yyyy-MM-dd HH:mm:ss.SSS"))

    val items = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .csv("rewards_receipts_item_lat_v2.csv")

    receipts.createOrReplaceTempView("receipts")
    items.createOrReplaceTempView("items")

    val startInstant = ZonedDateTime.now(ZoneOffset.UTC).minusMonths(numMonths).toInstant()

    if (report == TotalPurchaseByStore.fileName) {
      val totalByStore = new TotalPurchaseByStore(startInstant, spark)
      val writer = createWriter(outputFormat, TotalPurchaseByStore.fileName)
      writer(totalByStore.dataFrame)

    } else if (report == CategoryPopularity.fileName) {
      val categoryPopularity = new CategoryPopularity(startInstant, spark)
      val writer = createWriter(outputFormat, CategoryPopularity.fileName)
      writer(categoryPopularity.dataFrame)

    } else if (report == DeviceByYear.fileName) {
      val deviceByYear = new DeviceByYear(startInstant, spark)
      val writer = createWriter(outputFormat, DeviceByYear.fileName)
      writer(deviceByYear.dataFrame)

    }
  }

  def main(args: Array[String]) {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("spark-engineer"),
        opt[Boolean]('l', "local")
          .action((x, c) => c.copy(local = x))
          .text("run in local or cluster mode"),
        opt[String]('o', "output-format")
          .action((x, c) => c.copy(outputFormat = x))
          .text("format of the output: csv, json, or parquet"),
        opt[String]('r', "report")
          .action((x, c) => c.copy(report = x))
          .text("which report to run: category_popularity_by_store, device_by_year, total_purchase_by_store"),
        opt[Int]('n', "num-months")
          .action((x, c) => c.copy(numMonths = x))
          .text("calculate based on n-many months in the past")
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        runReports(config.local, config.outputFormat, config.report, config.numMonths)
      case _ =>
    }
  }
}

case class Config(
                   local: Boolean = true,
                   outputFormat: String = "json",
                   report: String = "total_purchase_by_store",
                   numMonths: Int = 12
                 )