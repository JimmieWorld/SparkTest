package net.james_world

case class Event(
    fileName: String, // Название файла (источник события)
    eventType: String, // Тип события (SESSION_START, DOC_OPEN, QS, CARD_SEARCH_START)
    timestamp: String, // Временная метка
    queryId: Option[String] = None, // Идентификатор запроса (возможно)
    query: Option[Seq[String]] = None, // Запрос (для QS или CARD_SEARCH_START)
    documentId: Option[String] = None, // ID документа (для QS или CARD_SEARCH)
    relatedDocuments: Seq[String] = Seq.empty // Список связанных документов (для QS или CARD_SEARCH)
)