package com.hootsuite.akka.persistence.redis.journal

import akka.util.ByteString
import org.apache.commons.codec.binary.Base64
import redis.ByteStringFormatter
import spray.json._

/**
 * Journal entry that can be serialized and deserialized to JSON
 * JSON in turn is serialized to ByteString so it can be stored in Redis with Rediscala
 */
case class Journal(sequenceNr: Long, persistentRepr: Array[Byte], deleted: Boolean)

object Journal {
  import DefaultJsonProtocol.{jsonFormat3, LongJsonFormat,BooleanJsonFormat}

  implicit val byteArrayFormat = new JsonFormat[Array[Byte]] {
    override def write(ba: Array[Byte]): JsValue = {
      JsString(Base64.encodeBase64String(ba))
    }
    override def read(json: JsValue): Array[Byte] = json match {
      case JsString(s) =>
        try {
          Base64.decodeBase64(s)
        } catch {
          case exp: Throwable => deserializationError("Cannot deserialize persistentRepr in Journal", exp)
        }
      case _ => deserializationError("Cannot find deserializable JsValue")
    }
  }

  implicit val fmt: JsonFormat[Journal] = jsonFormat3(Journal.apply)

  implicit val byteStringFormatter = new ByteStringFormatter[Journal] {
    override def serialize(data: Journal): ByteString = {
      ByteString(data.toJson.compactPrint)
    }

    override def deserialize(bs: ByteString): Journal = {
      try {
        bs.utf8String.parseJson.convertTo[Journal]
      } catch {
        case e: Exception => deserializationError("Error deserializing Journal.", e)
      }
    }
  }
}
