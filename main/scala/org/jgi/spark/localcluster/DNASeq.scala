package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/12/17.
  */

import java.nio.ByteBuffer
import java.util.Base64

import de.greenrobot.common.hash.Murmur3A


@SerialVersionUID(51114L)
class DNASeq(val bytes: Array[Byte]) extends Ordered[DNASeq] with Serializable {

  def to_bases: String = {
    bytes.map(DNASeq.int8_to_uint8).flatMap(i =>
      List((i >>> 6) % 4, (i >>> 4) % 4, (i >>> 2) % 4, i % 4)).map(DNASeq.inverse_base_encoding(_)).mkString
  }

  def to_bases(k: Int): String = {
    to_bases.take(k)
  }

  def to_base64: String = {
    DNASeq.base64_encoder.encodeToString(bytes)
  }

  override def equals(that: Any): Boolean = {
    that match {
      case seq: DNASeq =>

        //        if (this.bytes.length != seq.bytes.length) return false
        //        (0 until bytes.length).foreach { i =>
        //          if (bytes(i) != seq.bytes(i)) return false
        //        }
        //        return true
        bytes.deep == seq.bytes.deep
      case _ => false
    }
  }

  private def hashCode_old: Int = {
    if (bytes.length >= 4) {
      val v = java.nio.ByteBuffer.wrap(bytes.take(4)).getInt()
      if (v > 0) v else -v
    } else {
      var h: Int = 0
      bytes.indices.foreach { i => h = (h << 8) + bytes(i) }
      if (h > 0) h else -h
    }
  }

  override def hashCode: Int = {
    val murmur3a = new Murmur3A()
    murmur3a.update(bytes)
    val h = murmur3a.getValue.toInt
    if (h > 0) h else -h
  }

  override def toString: String = {
    this.to_bases
  }

  def compare(that: DNASeq): Int = {
    for (i <- 0 until math.min(this.bytes.length, that.bytes.length)) {
      val a = this.bytes(i)
      val b = that.bytes(i)
      if (a > b) return 1
      if (a < b) return -1
    }
    if (this.bytes.length > that.bytes.length)
      1
    else if (this.bytes.length < that.bytes.length)
      -1
    else
      0
  }
}

object DNASeq {
  val base_encoding = Map('A' -> 0, 'T' -> 1, 'C' -> 2, 'G' -> 3)
  val inverse_base_encoding = Map(0 -> 'A', 1 -> 'T', 2 -> 'C', 3 -> 'G')
  val rc = Map('A' -> 'T', 'T' -> 'A', 'C' -> 'G', 'G' -> 'C', 'N' -> 'N')

  val base64_encoder: Base64.Encoder = Base64.getEncoder
  val base64_decoder: Base64.Decoder = Base64.getDecoder

  def reverse_complement(s:String) ={
    s.reverse.map(DNASeq.rc(_))
  }
  def long_to_bytes(i: Long): Array[Byte] = {
    ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE).putLong(i).array
  }

  def long_to_base64(i: Long): String = {
    this.base64_encoder.encodeToString(long_to_bytes(i))
  }

  def base64_to_long(s: String): Long = {
    ByteBuffer.wrap(base64_decoder.decode(s)).getLong()
  }

  def int_to_bytes(i: Int): Array[Byte] = {
    ByteBuffer.allocate(java.lang.Integer.SIZE / java.lang.Byte.SIZE).putInt(i).array
  }

  def int_to_base64(i: Int): String = {
    this.base64_encoder.encodeToString(int_to_bytes(i))
  }

  def base64_to_int(s: String): Int = {
    ByteBuffer.wrap(base64_decoder.decode(s)).getInt()

  }

  def int8_to_uint8(unsingedInt: Byte): Int = {
    if (unsingedInt < 0) unsingedInt + 256 else unsingedInt.toInt
  }

  def from_base64(base64String: String): DNASeq = {
    new DNASeq(DNASeq.base64_decoder.decode(base64String))
  }

  def from_bases(dnaseq: String): DNASeq = {
    //one byte for 4 bases

    val bytes = dnaseq.map(base_encoding(_))
      .grouped(4)
      .map { fourints => {
        var (a, b, c, d) = (0, 0, 0, 0)
        if (fourints.nonEmpty) a = fourints(0)
        if (fourints.size > 1) b = fourints(1)
        if (fourints.size > 2) c = fourints(2)
        if (fourints.size > 3) d = fourints(3)
        (a << 6) + (b << 4) + (c << 2) + d
      }
      }.map(_.toByte).toArray
    new DNASeq(bytes)
  }
}
