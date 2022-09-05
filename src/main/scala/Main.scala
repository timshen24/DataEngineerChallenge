import SparkSessionize._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val cleansedDF = prepareData()
      .where('request_processing_time > 0 && 'backend_processing_time > 0 && 'response_processing_time > 0)
    val sessionWindowDF = cleansedDF
      .select('timestamp, split('client_port, ":").getItem(0).as("ip_address"), split('request, " ").getItem(1).as("url"))
      .withColumn("last_timestamp", lag('timestamp, 1) over Window.partitionBy('ip_address).orderBy('timestamp))
      .select('timestamp, 'ip_address, 'url, ((unix_timestamp('timestamp) - unix_timestamp('last_timestamp)) / 60).as
      ("time_diff_minutes"))
      .select('timestamp, 'ip_address, 'url, when('time_diff_minutes.isNull, lit(0d)).otherwise('time_diff_minutes).as("time_diff_minutes"))
      .withColumn("is_new_session", when('time_diff_minutes > lit(15), lit(1)).otherwise(lit(0)))
      .withColumn("session_id", sum('is_new_session) over Window.partitionBy('ip_address).orderBy('timestamp))
    //    sessionWindowDF.show(truncate = false)
    sessionWindowDF.cache()
    val sessionWindowDF2 = sessionWindowDF.select('ip_address, 'session_id).distinct().withColumn("uuid", expr("uuid()"))
    //    sessionWindowDF2.show(truncate = false)

    /**
     * +--------------------------+-------------+------------------------------------------------------------------------------------------------------------------------+------------------------------------+
     * |timestamp                 |ip_address   |url                                                                                                                     |session_id                          |
     * +--------------------------+-------------+------------------------------------------------------------------------------------------------------------------------+------------------------------------+
     * |2015-07-23 00:20:34.182397|1.186.180.183|https://paytm.com:443/shop/h/electronics?utm_source=Affiliates&utm_medium=VCOMM&utm_campaign=VCOMM-generic&utm_term=1064|0e2bff98-ce5d-4982-a893-b215b769a414|
     * |2015-07-23 05:08:59.102895|1.186.180.183|https://paytm.com:443/shop/h/electronics?utm_source=Httpool&utm_medium=Affiliate&utm_campaign=Httpool-mp&utm_term=1028  |c2515086-4260-4b4a-90f4-9c4d69933052|
     * |2015-07-23 05:09:02.472297|1.186.180.183|https://paytm.com:443/mobilebill                                                                                        |c2515086-4260-4b4a-90f4-9c4d69933052|
     * |2015-07-23 05:09:13.628479|1.186.180.183|https://paytm.com:443/dth                                                                                               |c2515086-4260-4b4a-90f4-9c4d69933052|
     * |2015-07-23 05:09:24.494398|1.186.180.183|https://paytm.com:443/terms                                                                                             |c2515086-4260-4b4a-90f4-9c4d69933052|
     * ......
     * +--------------------------+-------------+------------------------------------------------------------------------------------------------------------------------+------------------------------------+
     */
    val task1 = sessionWindowDF.join(sessionWindowDF2, Seq("ip_address", "session_id"))
      .select('timestamp, 'ip_address, 'url, 'uuid.as("session_id"))
    task1.show(truncate = false)

    val task1WithMinMaxTs = task1.select('timestamp, 'ip_address, 'session_id, min('timestamp).over(Window.partitionBy('ip_address,
      'session_id)).as("min"), max('timestamp).over(Window.partitionBy('ip_address, 'session_id)).as("max"))
      .select('ip_address, 'session_id, ((unix_timestamp('max) - unix_timestamp('min)) / 60).as("duration_minutes"))
      .distinct()

    /**
     * +-------------+------------------------------------+------------------+
     * |ip_address   |session_id                          |duration_minutes  |
     * +-------------+------------------------------------+------------------+
     * |1.186.180.183|c2515086-4260-4b4a-90f4-9c4d69933052|0.4166666666666667|
     * |1.186.180.183|0e2bff98-ce5d-4982-a893-b215b769a414|0.0               |
     * ......
     * +-------------+------------------------------------+------------------+
     */
    val task2 = task1WithMinMaxTs.agg(avg('duration_minutes))
    task2.show(truncate = false)

    /**
     * +-------------+------------------------------------+-----------+
     * |ip_address   |session_id                          |unique_urls|
     * +-------------+------------------------------------+-----------+
     * |1.186.180.183|0e2bff98-ce5d-4982-a893-b215b769a414|1          |
     * |1.186.180.183|c2515086-4260-4b4a-90f4-9c4d69933052|4          |
     * +-------------+------------------------------------+-----------+
     * ......
     */
    val task3 = task1
      .withColumn("unique_urls", approx_count_distinct('url) over Window.partitionBy('ip_address, 'session_id))
      .select('ip_address, 'session_id, 'unique_urls)
      .distinct()
    task3.show(truncate = false)

    /**
     * +-------------+------------------------------------+------------------+
     * |ip_address   |session_id                          |duration_minutes  |
     * +-------------+------------------------------------+------------------+
     * |1.186.180.183|c2515086-4260-4b4a-90f4-9c4d69933052|0.4166666666666667|
     * |1.186.180.183|0e2bff98-ce5d-4982-a893-b215b769a414|0.0               |
     * ......
     * +-------------+------------------------------------+------------------+
     */
    val task4 = task1WithMinMaxTs.sort('duration_minutes.desc)
    task4.show(truncate = false)
  }
}
