import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import scalax.file.Path

object Index {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("spark-csv-sample").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    val dirName: String = "newcars"
    val path: Path = Path(dirName)
    if (path.exists) {
      path.deleteRecursively()
      println("Dir exists")
    } else {
      println("Dir not exists")
    }

    val df = sqlContext.read.format("com.databricks.spark.csv")
              .option("header", "true")
              .load("cars.csv")
    df.select("year", "model").write.format("com.databricks.spark.csv").save(dirName)
    sc.stop()
  }
}
