package queries

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, element_at, rank}
import org.apache.spark.sql.{SparkSession, functions}

import java.time.Instant

/** Report to generate the top categories of products at stores. This report assumes that the most specific product
 * description is the last element in the category column.
 *
 * @param startInstant the beginning of the date window
 * @param spark
 */
class CategoryPopularity(startInstant: Instant, spark: SparkSession) extends BaseQuery(startInstant, spark) {
  val windowSpec = Window.partitionBy("store_id", "purchase_date").orderBy(col("count").desc)
  val dataFrame = spark.sql(s"""
        with filtered_receipts as (
            select store_id, RECEIPT_ID, store_name, RECEIPT_TOTAL, date(RECEIPT_PURCHASE_DATE) as purchase_date
            from receipts_cleaned
            where RECEIPT_PURCHASE_DATE >= '${startTimestamp}'
        )
        select * from filtered_receipts r
        join items i on r.RECEIPT_ID = i.REWARDS_RECEIPT_ID
    """)
    .withColumn("category_array", functions.split(col("CATEGORY"), "\\|"))
    .withColumn("last_elm", element_at(col("category_array"), -1))
    .select(col("store_id"), col("last_elm"), col("purchase_date"))
    .filter("last_elm is not null")
    .groupBy("store_id", "last_elm", "purchase_date")
    .count()
    .withColumn("rank", rank().over(windowSpec))
    .orderBy(col("store_id"), col("purchase_date"), col("rank").asc)
}

object CategoryPopularity {
  val fileName = "category_popularity_by_store"
}
