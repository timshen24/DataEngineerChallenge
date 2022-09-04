import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class OrderTesting extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  case class Order(
                    order_id: Option[String] = None,
                    created_time_utc: Option[Timestamp] = None,
                    product_id: Option[String] = None,
                    user_id: Option[String] = None,
                    store_id: Option[String] = None,
                    country: Option[String] = None,
                    gmv: Option[Double] = None,
                    payment_time_utc: Option[Timestamp] = None
                  )

  implicit def toOption[T](v: T): Some[T] = Some(v) // to avoid wrapping all values in Some

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .config(new SparkConf())
    .getOrCreate()

  import spark.implicits._

  val ts: Seq[Timestamp] = Seq(new Timestamp(1649248583), new Timestamp(1648248583), new Timestamp(1649244583), new Timestamp(1649248383))

  def calculatePaidOrdersGmv(orders: DataFrame): Unit = ???

  def check(result: Unit): Any = ???

  test("calculatePaidOrdersGmv should only take into account paid orders") {
    val orders = Seq(
      Order(created_time_utc = ts(1), country = "DE", gmv = 5.5, payment_time_utc = ts(2)),
      Order(created_time_utc = ts(1), country = "DE", gmv = 3.5, payment_time_utc = ts(3)),
      // This order should not be counted
      Order(created_time_utc = ts(1), country = "DE", gmv = 2.5, payment_time_utc = None)
    ).toDF()

    val result = calculatePaidOrdersGmv(orders)
    check(result)
  }
}
