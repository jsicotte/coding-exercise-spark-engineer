package queries

import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset}

class TotalPurchaseByStore(var startInstant: Instant, var spark: SparkSession) {
  val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
  val startTimestamp = formatter.format(startInstant)

  // In some cases a store_number or address can identify a store. There are also instances
  // where both of these fields are null but there is a store_phone.
  // I have also noticed garbage values in store_city and store_state, so all columns are suspect.
  // There does not seem a way to use store_phone to fill in missing data.

  // total dollar amount per store by date
  val dataFrame = spark.sql(s"""
    with filtered_stores as (
        select COALESCE(store_address, store_number, store_phone) as store_id, store_name, RECEIPT_TOTAL, RECEIPT_PURCHASE_DATE
        from receipts as r
        where not (store_address is null and store_number is null and store_phone is null)
        and RECEIPT_PURCHASE_DATE >= '${startTimestamp}'
    ),
    sums as (
        select store_id, store_name, sum(RECEIPT_TOTAL) as total from filtered_stores group by store_id, store_name
    ),
    top_ten as (
        select store_id from sums order by total desc limit 10
    )
    select f.store_id, date(RECEIPT_PURCHASE_DATE), sum(RECEIPT_TOTAL) as daily_total from filtered_stores f
    join top_ten t on f.store_id = t.store_id
    group by 1, 2
  """)
}

object TotalPurchaseByStore {
  val fileName = "total_purchase_by_store"
}
