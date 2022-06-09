import org.apache.spark.sql.SparkSession

object wordcount {
  def main(args: Array[String]): Unit = {

    // Hello this is spark code Hello Spark
    /**
     * Hello 2
     * This 1
     * is 1
     *
     */
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val text_file = spark.sparkContext.textFile("src/main/resources/com/unravel/sparktutorial/data.txt")
    text_file.flatMap(line => line.split(" "))
      .map(word => (word, 1)).reduceByKey((a,b) => a + b).foreach(println)


    //val counts = text_file.flatMap(line => line.split(" "))
    //  .map(word => (word, 1))
    //.reduceByKey((a,b) => a + b)
    //counts.foreach(println)

  }
}