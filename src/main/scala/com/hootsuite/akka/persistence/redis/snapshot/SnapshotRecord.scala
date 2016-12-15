package com.hootsuite.akka.persistence.redis.snapshot

import akka.util.ByteString
import org.apache.commons.codec.binary.Hex
import spray.json._
import redis.ByteStringFormatter

/**
 * Snapshot entry that can be serialized and deserialized to JSON
 * JSON in turn is serialized to ByteString so it can be stored in Redis with Rediscala
 */
case class SnapshotRecord(sequenceNr: Long, timestamp: Long, snapshot: Array[Byte])

object SnapshotRecord {
  import DefaultJsonProtocol.{jsonFormat3, LongJsonFormat}

  implicit val byteArrayFormat = new JsonFormat[Array[Byte]] {
    override def write(ba: Array[Byte]): JsValue = {
      JsString(Hex.encodeHexString(ba).toUpperCase)
    }
    override def read(json: JsValue): Array[Byte] = json match {
      case JsString(s) =>
        try {
          Hex.decodeHex(s.toCharArray)
        } catch {
          case exp: Throwable => deserializationError("Cannot deserialize snapshot in SnapshotRecord", exp)
        }
      case _ => deserializationError("Cannot find deserializable JsValue")
    }
  }

  implicit val fmt: JsonFormat[SnapshotRecord] = jsonFormat3(SnapshotRecord.apply)

  implicit val byteStringFormatter = new ByteStringFormatter[SnapshotRecord] {
    override def serialize(data: SnapshotRecord): ByteString = {
      ByteString(data.toJson.compactPrint)
    }

    override def deserialize(bs: ByteString): SnapshotRecord = {
      try {
        bs.utf8String.parseJson.convertTo[SnapshotRecord]
      } catch {
        case e: Exception => deserializationError("Error deserializing SnapshotRecord.", e)
      }
    }
  }
}
