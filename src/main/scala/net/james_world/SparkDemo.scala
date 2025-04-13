package net.james_world

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object SparkDemo {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
        .master("local[*]")
        .appName("SparkDemo")
        .getOrCreate()

        // Чтение данных
        val logsPath = "src/main/resources/data"
        val files = new java.io.File(logsPath).listFiles()
//        val logFiles = spark.sparkContext.wholeTextFiles(logsPath)

        val sessionsRDD = spark.sparkContext.parallelize(
            files.flatMap(file => LogParser.parseLogFile(file))
        )

        // По хорошему реализовать на стороне LogParser, но пока так
        val rowRDD = sessionsRDD.map(event => Row(
            event.fileName,
            event.eventType,
            event.timestamp,
            event.queryId.orNull, // Преобразуем Option[String] в String (null, если None)
            event.query.orNull, // Преобразуем Option[Seq[String]] в Seq[String] (null, если None)
            event.documentId.orNull, // Преобразуем Option[String] в String (null, если None)
            event.relatedDocuments
        ))

        val schema = StructType(Array(
            StructField("fileName", StringType, nullable = false),
            StructField("eventType", StringType, nullable = false),
            StructField("timestamp", StringType, nullable = false),
            StructField("queryId", StringType, nullable = true),
            StructField("query", ArrayType(StringType, containsNull = true), nullable = true),
            StructField("documentId", StringType, nullable = true),
            StructField("relatedDocuments", ArrayType(StringType, containsNull = true), nullable = true)
        ))
        val df = spark.createDataFrame(rowRDD, schema)

        println(df.columns.mkString("Array(", ", ", ")"))
        import spark.implicits._
        println(df.where($"fileName" === "6060").show(truncate = false))

        val analyzer = new MetricsCalculator(df, spark)

        println(analyzer.countDocumentCardSearches("ACC_45616"))
        println(analyzer.countDocOpensByQS().show(10))
        spark.stop()
    }
}