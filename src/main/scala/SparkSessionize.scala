import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkSessionize {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .config(new SparkConf()
      .setAppName("spark-sessionize-job")
      .set("spark.sql.adaptive.enabled", "true"))
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val isDebug = false

  def prepareData(): DataFrame = {
    val schema = StructType(List(
      StructField("timestamp", TimestampType),
      StructField("elb", StringType),
      StructField("client_port", StringType),
      StructField("backend_port", StringType),
      StructField("request_processing_time", DoubleType),
      StructField("backend_processing_time", DoubleType),
      StructField("response_processing_time", DoubleType),
      StructField("elb_status_code", StringType),
      StructField("backend_status_code", StringType),
      StructField("received_bytes", IntegerType),
      StructField("sent_bytes", IntegerType),
      StructField("request", StringType),
      StructField("user_agent", StringType),
      StructField("ssl_cipher", StringType),
      StructField("ssl_protocol", StringType)
    ))

    val rawDF = spark.read.format("csv")
      .options(Map("delimiter" -> " ", "header" -> "false", "encoding" -> "UTF-8"))
      .schema(schema)
      .load("data")
    val selectCertainRows: DataFrame => DataFrame = _.where('ip_address <=> lit("1.186.180.183"))


    if (isDebug) {
      rawDF.summary().show
      /**
       * +-------+----------------+------------------+------------+-----------------------+-----------------------+------------------------+-----------------+-------------------+------------------+------------------+--------------------+--------------------+--------------------+------------+
       * |summary|             elb|       client_port|backend_port|request_processing_time|backend_processing_time|response_processing_time|  elb_status_code|backend_status_code|    received_bytes|        sent_bytes|             request|          user_agent|          ssl_cipher|ssl_protocol|
       * +-------+----------------+------------------+------------+-----------------------+-----------------------+------------------------+-----------------+-------------------+------------------+------------------+--------------------+--------------------+--------------------+------------+
       * |  count|         1158500|           1158500|     1158500|                1158500|                1158500|                 1158500|          1158500|            1158500|           1158500|           1158500|             1158500|             1158495|             1158500|     1158500|
       * |   mean|            null|              null|        null|   -1.17051348295186...|    0.03305756986361663|    -1.18995419076374...|216.7690315062581| 216.69855416486837|27.532892533448425|  5488.19137850669|                null|                null|                null|        null|
       * | stddev|            null|              null|        null|   0.011824675161465738|    0.36602516685578956|    0.011824654382179903|42.31295192169145|  42.25417090646583|204.49287327553898|15629.351806091025|                null|                null|                null|        null|
       * |    min|marketpalce-shop|1.186.101.79:50613|           -|                   -1.0|                   -1.0|                    -1.0|              200|                  0|                 0|                 0|"GET http://paytm...|\tMozilla/5.0 (Wi...|                   -|           -|
       * |    25%|            null|              null|        null|                 2.1E-5|                 0.0029|                  2.0E-5|            200.0|              200.0|                 0|               213|                null|                null|                null|        null|
       * |    50%|            null|              null|        null|                 2.3E-5|               0.004986|                  2.1E-5|            200.0|              200.0|                 0|               709|                null|                null|                null|        null|
       * |    75%|            null|              null|        null|                 2.4E-5|               0.012311|                  2.2E-5|            200.0|              200.0|                 0|              9518|                null|                null|                null|        null|
       * |    max|marketpalce-shop|  99.8.170.3:60015|10.0.6.99:81|                1.51E-4|              58.698117|                0.001433|              504|                504|             23403|           1068957|PUT https://paytm...|ultrafone 105+(Li...|Mozilla/5.0 (comp...|     TLSv1.2|
       * +-------+----------------+------------------+------------+-----------------------+-----------------------+------------------------+-----------------+-------------------+------------------+------------------+--------------------+--------------------+--------------------+------------+
       *
       */
      rawDF.transform(selectCertainRows)
    } else {
      rawDF
    }
  }
}
