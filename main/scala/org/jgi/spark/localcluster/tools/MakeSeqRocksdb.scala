/**
  * Created by Lizhen Shi on 6/8/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.jgi.spark.localcluster.{Utils}
import sext._
import org.rocksdb.Options
import org.rocksdb.CompactionStyle
import org.rocksdb.CompressionType
import org.rocksdb.RocksDB
import org.rocksdb.util.SizeUnit

import scala.io.Source

object MakeSeqRocksdb extends App with LazyLogging {

  case class Config(seq_file: String = "", db_file: String = "",
                    compress: Boolean = false)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("MakeSeqRocksdb") {
      head("MakeSeqRocksdb", Utils.VERSION)

      opt[String]('i', "seq_file").required().valueName("<file>").action((x, c) =>
        c.copy(seq_file = x)).text("input seq file")

      opt[String]("db_file").required().valueName("<file>").action((x, c) =>
        c.copy(db_file = x)).text("rocksdb file")

      opt[Unit]("compress").action((_, c) =>
        c.copy(compress = true)).text("apply canonical kmer")

      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }


  def run(config: Config): Unit = {

    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": Program started ...")
    RocksDB.loadLibrary()

    val source = Source.fromFile(config.seq_file)

    val compression_type = if (config.compress) CompressionType.LZ4_COMPRESSION else CompressionType.NO_COMPRESSION

    val options = new Options().setCreateIfMissing(true)
      .setWriteBufferSize(8 * SizeUnit.KB).setMaxWriteBufferNumber(10)
      .setMaxBackgroundCompactions(10)
      .setCompressionType(compression_type)
      .setCompactionStyle(CompactionStyle.UNIVERSAL)
    val db = RocksDB.open(options, config.db_file)

    for (line <- source.getLines()) {
      val lst = line.split('\t')
      val no = lst(0).toInt
      val seq = lst(2)
      val key = Utils.toByteArray(no)
      val val_bytes = seq.getBytes
      db.put(key, val_bytes)
    }

    source.close()
    db.close()
    options.close()

    val totalTime1 = System.currentTimeMillis
    logger.info("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))


  }


  override def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logger.info(s"called with arguments\n${options.valueTreeString}")

        run(config)
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}
