import org.apache.spark.sql.SparkSession

object wordcount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val text_file = spark.sparkContext.textFile("src/main/resources/com/unravel/sparktutorial/data.txt")
    val counts = text_file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
    .reduceByKey((a,b) => a + b)
    counts.foreach(println)

  }
}