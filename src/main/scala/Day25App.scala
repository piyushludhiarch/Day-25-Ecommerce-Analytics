import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day25App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Ecommerce Analytics")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    val customers = Seq(
      (1,"Amit","Hyderabad"),
      (2,"Sara","Delhi"),
      (3,"Rahul","Mumbai")
    ).toDF("customer_id","customer","city")

    val products = Seq(
      (101,"Laptop","Electronics",60000),
      (102,"Mouse","Accessories",800),
      (103,"Phone","Electronics",30000),
      (104,"Keyboard","Accessories",1500)
    ).toDF("product_id","product","category","price")

    val orders = Seq(
      (1001,1,101,1),
      (1002,1,102,2),
      (1003,2,103,1),
      (1004,3,104,3),
      (1005,2,101,1)
    ).toDF(
      "order_id",
      "customer_id",
      "product_id",
      "quantity"
    )

    println("===== E-COMMERCE ANALYTICS =====")

    val data = orders
      .join(customers, Seq("customer_id"))
      .join(products, Seq("product_id"))
      .withColumn(
        "revenue",
        col("quantity") * col("price")
      )

    println("ORDER DATA:")
    data.show(false)

    println("CATEGORY REVENUE:")

    data.groupBy("category")
      .agg(
        sum("revenue").as("revenue"),
        sum("quantity").as("units_sold")
      )
      .orderBy(desc("revenue"))
      .show(false)

    println("CUSTOMER REVENUE:")

    val customerRevenue = data
      .groupBy("customer_id","customer")
      .agg(sum("revenue").as("revenue"))

    customerRevenue.show(false)

    println("CUSTOMER RANK:")

    val w = Window.orderBy(desc("revenue"))

    customerRevenue
      .withColumn("rank", dense_rank().over(w))
      .show(false)

    println("TOP PRODUCTS:")

    data.groupBy("product_id","product")
      .agg(sum("revenue").as("revenue"))
      .orderBy(desc("revenue"))
      .show(false)

    spark.stop()
  }
}
