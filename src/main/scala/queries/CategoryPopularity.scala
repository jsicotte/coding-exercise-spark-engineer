package queries

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, element_at, rank}

import java.util.Date

class CategoryPopularity(var startDate: Date, var spark: SparkSession) {
  val windowSpec = Window.partitionBy("store_id", "purchase_date").orderBy(col("count").desc)
  val dataFrame = spark.sql("""
        with filtered_receipts as (
            select COALESCE(store_address, store_number, store_phone) as store_id,RECEIPT_ID, store_name, RECEIPT_TOTAL, date(RECEIPT_PURCHASE_DATE) as purchase_date
            from receipts as r
            where not (store_address is null and store_number is null and store_phone is null)
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
