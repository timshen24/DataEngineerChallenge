import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object SparkSessionize extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .config(new SparkConf()
      .setAppName("spark-sessionize-job")
      .set("spark.sql.adaptive.enabled", "true"))
    .getOrCreate()

  import spark.implicits._

  // shortcut for DF to DS
  def toDS[T <: Product : Encoder](df: DataFrame): Dataset[T] = df.as[T]

  spark.read.text("data")
    .select(split('value, " "))
//    .toDF("timestamp", "elb", "client_port", "backend_port")
    .show(truncate = false)

}
