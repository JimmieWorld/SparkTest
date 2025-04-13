package net.james_world

import java.io.File
import scala.io.Source
import scala.util.Using
import scala.util.matching.Regex

object Patterns {
    val documentPattern: Regex = "[A-Z]+_\\d+".r
    val timestampPattern: Regex = "\\d{2}\\.\\d{2}\\.\\d{4}_\\d{2}:\\d{2}:\\d{2}".r
    val searchSearchResultPattern: Regex = "-?\\d+".r
    val quickSearchQueryPattern: Regex = "\\{([^\\}]+)\\}".r

//    private val documentIdPattern = "[A-Z]+_\\d+".r
//
//    def extractDocumentId(line: String): Option[String] = {
//        documentIdPattern.findFirstIn(line)
//    }
}

object LogParser {
    def parseLogFile(file: File): Seq[Event] = {
        val fileName = file.getName
        // Получаем коллекцию строк из файла или пусту стую коллекцию
        // в случае неудачи
        val lines = Using(Source.fromFile(file, "Windows-1251")) { source =>
            source.getLines().toSeq
        }.getOrElse(Seq.empty)

        // Состояния для отслеживания карточного поиска
        var isCardSearchActive = false
        var isCardOrQuickSearchEnded = false
        var cardSearchBuffer = Seq.empty[String]
        var quickSearchBuffer = Seq.empty[String]
        var queryIdLine: String = ""

        val events = lines.flatMap { line =>
            if (line.trim.startsWith("CARD_SEARCH_START")) {
                // Начало карточного поиска
                isCardSearchActive = true
                cardSearchBuffer = Seq(line.trim) // Начинаем буферизацию
                None
            } else if (line.trim.startsWith("CARD_SEARCH_END")) {
                // Конец карточного поиска
                isCardSearchActive = false
                isCardOrQuickSearchEnded = true
                None
            } else if (line.trim.startsWith("QS")) {
                // Строка быстрого поиска
                quickSearchBuffer = Seq(line.trim)
                isCardOrQuickSearchEnded = true
                None
            } else if (isCardSearchActive) {
                // Продолжение карточного поиска
                cardSearchBuffer :+= line.trim
                None
            } else if (isCardOrQuickSearchEnded) {
                // Следующая строка после CARD_SEARCH_END или QS содержит queryId
                queryIdLine = line.trim
                isCardOrQuickSearchEnded = false // Сбрасываем флаг
                if (cardSearchBuffer.nonEmpty) {
                    val fullCardSearch = cardSearchBuffer.mkString("\n") // Объединяем все строки
                    cardSearchBuffer = Seq.empty // Очищаем буфер
                    parseCardSearchEvents(fullCardSearch, queryIdLine, fileName) // Парсим событие с queryId
                } else if (quickSearchBuffer.nonEmpty) {
                    val fullQuickSearch = quickSearchBuffer.mkString("\n") // Объединяем все строки
                    quickSearchBuffer = Seq.empty // Очищаем буфер
                    parseCardQuickSearch(fullQuickSearch, queryIdLine, fileName)
                } else None
            } else {
                // Обработка других событий
                parseOtherEvent(line.trim, fileName)
            }
        }

        processEvents(events)
    }

    private def processEvents(events: Seq[Any]): Seq[Event] = {
        events.flatMap {
            case Some(event: Event) => Some(event) // Если элемент — Option[Event]
            case event: Event => Some(event)      // Если элемент — Event
            case _ => None                        // Игнорируем все остальные случаи
        }
    }

    private def parseOtherEvent(line: String, fileName: String): Seq[Event] = {
        val parts = line.split("\\s+", 3)
        if (parts.length < 2) Seq()
        else {
            val eventType = parts(0)
            val timestamp = extractTimestamp(parts(1))

            eventType match {
                case "SESSION_START" | "SESSION_END" =>
                    Seq(Event(fileName, eventType, timestamp))

                case "DOC_OPEN" =>
                    Seq(Event(
                        fileName = fileName,
                        eventType = eventType,
                        timestamp = timestamp,
                        queryId = extractQueryId(parts(2)),
                        documentId = extractDocumentId(line),
                        relatedDocuments = Seq.empty
                    ))

                case _ =>
                    Seq()
            }
        }
    }

    private def parseCardQuickSearch(fullQuickSearch: String, queryIdLine: String, fileName: String): Seq[Event] = {

        val parts = fullQuickSearch.split("\n")
        val timestamp = extractTimestamp(parts(0)) // Временная метка из QS
        val queryId = extractQueryId(queryIdLine) // номер результата поиска

        // Создаем событие QS
        val quickSearchEvent = Event(
            fileName = fileName,
            eventType = "QS",
            timestamp = timestamp,
            queryId = queryId,
            query = Some(Seq(extractQuickSearchQuery(parts(0)).stripPrefix("{").stripSuffix("}"))),
            relatedDocuments = Seq.empty
        )

        val relatedDocuments = extractDocuments(queryIdLine)

        // Создаем событие для результата поиска
        val resultEvent = Event(
            fileName = fileName,
            eventType = "QS_RESULT",
            timestamp = timestamp,
            queryId = queryId,
            relatedDocuments = relatedDocuments
        )

        // Можно в принципе объединить события в одно, но нет достоверной информации о строке после поиска
        Seq(quickSearchEvent, resultEvent)
    }

    private def parseCardSearchEvents(fullCardSearch: String, queryIdLine: String, fileName: String): Seq[Event] = {
        val parts = fullCardSearch.split("\n")
        val timestamp = extractTimestamp(parts(0)) // Временная метка из CARD_SEARCH_START
        val queryId = extractQueryId(queryIdLine) // номер результата поиска

        // Извлекаем все запросы внутри блока
        val queries = parts.tail.flatMap(extractQuery)

        // Создаем событие CARD_SEARCH
        val cardSearchEvent = Event(
            fileName = fileName,
            eventType = "CARD_SEARCH",
            timestamp = timestamp,
            queryId = queryId,
            query = Some(queries),
            relatedDocuments = Seq.empty
        )

        val relatedDocuments = extractDocuments(queryIdLine)

        // Создаем событие для результата поиска
        val resultEvent = Event(
            fileName = fileName,
            eventType = "CARD_SEARCH_RESULT",
            timestamp = timestamp,
            queryId = queryId,
            relatedDocuments = relatedDocuments
        )

        // Можно в принципе объединить события в одно, но нет достоверной информации о строке после поиска
        Seq(cardSearchEvent, resultEvent)
    }

    private def extractQuery(line: String): Option[String] = {
        if (line.startsWith("$")) Some(line.stripPrefix("$")) else None
    }

    private def extractQueryId(line: String): Option[String] = {
        Patterns.searchSearchResultPattern.findFirstIn(line)
    }

    private def extractTimestamp(line: String): String = {
        Patterns.timestampPattern.findFirstIn(line).getOrElse("")
    }

    private def extractQuickSearchQuery(line: String): String = {
        Patterns.quickSearchQueryPattern.findFirstIn(line).getOrElse("")
    }

    private def extractDocuments(line: String): Seq[String] = {
        Patterns.documentPattern.findAllIn(line).toSeq
    }

    private def extractDocumentId(line: String): Option[String] = {
        Patterns.documentPattern.findFirstIn(line)
    }
}