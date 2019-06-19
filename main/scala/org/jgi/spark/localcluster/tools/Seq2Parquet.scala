package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.jgi.spark.localcluster.Utils
import sext._

/**
  * Created by Lizhen Shi on 5/15/17.
  */


object Seq2Parquet  extends App with  LazyLogging {

  case class Config(input: String = "", output: String = "", pattern: String = "", n_partition: Int = 0, coalesce: Boolean = false)

  def APPNAME = "Seq2Parquet"

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config](APPNAME) {
      head(APPNAME, Utils.VERSION)

      opt[String]('i', "input").required().valueName("<dir>").action((x, c) =>
        c.copy(input = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")

      opt[String]('p', "pattern").valueName("<pattern>").action((x, c) =>
        c.copy(pattern = x)).text("if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq")


      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")


      opt[Unit]("coalesce").action((_, c) =>
        c.copy(coalesce = true)).text("coalesce the output")


      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input, only applicable to local files")

      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }

  def run(config: Config, spark: SparkSession): Unit = {
    //import spark.implicits._

    val sc = spark.sparkContext
    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": started ...")

    val seqFiles = Utils.get_files(config.input.trim(), config.pattern.trim())
    logger.debug(seqFiles)

    val textRDD: RDD[String] =
      if (config.n_partition > 0)
        sc.textFile(seqFiles, minPartitions = config.n_partition)
      else
        sc.textFile(seqFiles)

    val readsRDD = textRDD.map {
      line => line.split("\t|\n")
    }.map { x => Row(x(1), x(0) + " " + x(2)) }
    val aStruct = new StructType(Array(StructField("head", StringType, nullable = false)
      , StructField("id_seq", StringType, nullable = false)))
    val df = spark.sqlContext.createDataFrame(readsRDD, aStruct)
    df.printSchema()
    (if (config.coalesce) df.coalesce(1) else df)
      .write.option("compression", "snappy").mode("overwrite").parquet(config.output)

  }


  override def main(args: Array[String]) {
    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get
        if (config.input.startsWith("hdfs:") && config.n_partition > 0) {
          println("do not set partition when use hdfs input. Change block size of hdfs instead")
          sys.exit(-1)
        }
        logger.info("called with arguments\n" + options.valueTreeString)
        val spark = SparkSession
          .builder()
          .appName(APPNAME)
          .getOrCreate()

        run(config, spark)

        spark.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}
