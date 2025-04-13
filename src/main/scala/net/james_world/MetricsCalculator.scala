package net.james_world

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class MetricsCalculator(df: DataFrame, spark: SparkSession) {
    import spark.implicits._
    /**
     * Метод для подсчета количества раз, когда искали определенный документ в карточке поиска.
     *
     * @param documentId Идентификатор документа (например, "ACC_45616").
     * @return Количество раз, когда искали указанный документ.
     */
    def countDocumentCardSearches(documentId: String): Long = {
        df.filter($"eventType" === "CARD_SEARCH")
        .filter(size(filter($"query", queryElement => queryElement.contains(documentId))) > 0)
        .count()
    }

    /**
     * Метод для подсчета количества открытий документов, найденных через быстрый поиск, за каждый день.
     *
     * @return DataFrame с результатами (дата, documentId, количество открытий).
     */
    def countDocOpensByQS(): DataFrame = {
        // Добавляем поле даты из timestamp
        val dfWithDate = df.withColumn("date", substring($"timestamp", 0, 10))

        // Поиск документов, найденных через быстрый поиск
        val qsResults = dfWithDate
        .filter($"eventType" === "QS_RESULT")
        .select($"queryId", $"relatedDocuments", $"date")
        .withColumn("document", explode($"relatedDocuments"))

        // Все события DOC_OPEN
        val docOpenEvents = dfWithDate
        .filter($"eventType" === "DOC_OPEN")
        .select($"queryId", $"documentId", $"date")

        // Соединение данных
        val joinedData = docOpenEvents.join(qsResults, Seq("queryId", "date"), "inner")
        .filter($"documentId" === $"document")

        // Группировка и подсчет
        joinedData.groupBy($"date", $"documentId").count().withColumnRenamed("count", "totalOpens")
    }
}