package com.hootsuite.akka.persistence.redis.journal

import akka.util.ByteString
import redis.ByteStringFormatter
import spray.json._
import DefaultJsonProtocol._
import akka.persistence.PersistentRepr
import akka.serialization.Serialization

/**
 * Journal entry that can be serialized and deserialized to JSON
 * JSON in turn is serialized to ByteString so it can be stored in Redis with Rediscala
 */
final case class Journal(sequenceNr: Long, persistentRepr: JsValue, deleted: Boolean)

trait JournalProtocol {
  import DefaultJsonProtocol._

  implicit object PersistentRepr extends RootJsonFormat[PersistentRepr] {

    def write(persistentRepr: PersistentRepr): JsObject = {
      persistentRepr.toJson.asJsObject
    }
    def read(value: JsValue): PersistentRepr = {
      value.convertTo[PersistentRepr]
    }
  }

  object Journal {

    implicit val byteArrayFormat = new JsonFormat[Array[Byte]] {
      override def write(ba: Array[Byte]): JsValue = {
        JsString(new String(ba))
      }

      override def read(json: JsValue): Array[Byte] = json match {
        case JsString(s) =>
          try {
            s.getBytes
          } catch {
            case exp: Throwable => deserializationError("Cannot deserialize persistentRepr in Journal", exp)
          }
        case _ => deserializationError("Cannot find deserializable JsValue")
      }
    }

    implicit val fmt: JsonFormat[Journal] = jsonFormat3(Journal)

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
}

object JournalProtocol extends JournalProtocol

//object Journal {
//  import DefaultJsonProtocol.{jsonFormat3, LongJsonFormat,BooleanJsonFormat}
//
//
//  implicit val byteArrayFormat = new JsonFormat[Array[Byte]] {
//    override def write(ba: Array[Byte]): JsValue = {
//      JsString(new String(ba))
//    }
//    override def read(json: JsValue): Array[Byte] = json match {
//      case JsString(s) =>
//        try {
//          s.getBytes
//        } catch {
//          case exp: Throwable => deserializationError("Cannot deserialize persistentRepr in Journal", exp)
//        }
//      case _ => deserializationError("Cannot find deserializable JsValue")
//    }
//  }
//
//  implicit val fmt: JsonFormat[Journal] = jsonFormat3(Journal.apply)
//
//  implicit val byteStringFormatter = new ByteStringFormatter[Journal] {
//    override def serialize(data: Journal): ByteString = {
//      ByteString(data.toJson.compactPrint)
//    }
//
//    override def deserialize(bs: ByteString): Journal = {
//      try {
//        bs.utf8String.parseJson.convertTo[Journal]
//      } catch {
//        case e: Exception => deserializationError("Error deserializing Journal.", e)
//      }
//    }
//  }
//}
