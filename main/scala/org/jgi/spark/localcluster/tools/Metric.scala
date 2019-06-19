package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.jgi.spark.localcluster.tools.Metric.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{Map, Set}
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.{DNASeq, Utils}
import sext._



object Metric extends App with LazyLogging {


  case class Config(key_file: String = "", lpa_file: String = "", output: String = "",flag:String = "",
                    n_partition: Int = 0, sleep: Int = 0, purity: Double = 0.95, completeness: Double = 0.80 )

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("Metric") {
      head("Metric", Utils.VERSION)

      opt[String]('k', "key_file").required().valueName("<file>").action((x, c) =>
        c.copy(key_file = x)).text("reads from answer")

      opt[String]("flag").valueName("").action((x, c) =>
        c.copy(flag = x)).
        validate(x =>
          if (Seq("mock","cami").contains(x.toLowerCase)) success
          else failure("should be one of <local|global>"))
        .text("clustering schema. <mock|cami>")

      opt[String]('l', "lpa_file").required().valueName("<file>").action((x, c) =>
        c.copy(lpa_file = x)).text("reads from lpa_input.  e.g. output from PurityCompletenes")

      opt[String]('o', "output").required().valueName("<file>").action((x, c) =>
        c.copy(output = x)).text("output to PurityCompletenes. e.g. output from Metric")

      opt[Int]('n', "n_partition").action((x, c) => c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")

      help("help").text("prints this usage text")


    }
    parser.parse(args, Config())
  }

  def logInfo(str: String) = {
    logger.info(str)
    println("AAAA " + str)
  }


  def run(config: Config,sc: SparkContext) = {

    sc.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")
/*
    val str = ">"
    val lpa = (if(config.flag.toLowerCase=="mock"){
          sc.textFile(config.lpa_file, minPartitions = config.n_partition)
            .map{line => line.split(str)}.map { x =>  x(1) }}
          else{
          sc.textFile(config.lpa_file, minPartitions = config.n_partition)})
          .map { line => line.split("\t|\n|>")}.map { x => (x(0), x(1)) }

*/

    val lpa = sc.textFile(config.lpa_file, minPartitions = config.n_partition)
             .map { line => line.split("\t|\n")}.map { x => (x(0), x(1)) }
    //lpa.take(100).foreach(println)
    val lpaSize=lpa.map(x => (x._2, x._1)).groupByKey().map(x=>(x._1,x._2.size))//(13,size)
    val l1=lpa.map(x => (x._2, x._1)).join(lpaSize).map(x=>(x._2._1,(x._1,x._2._2)))//(reads,(13,size))

    val key = sc.textFile(config.key_file, minPartitions = config.n_partition)
             .map { line => line.split("\t|\n")}.map { x => (x(0), x(1)) }
    val keySize=key.map(x => (x._2, x._1)).groupByKey().map(x=>(x._1,x._2.size))//(Ni,size)
    val k1=key.map(x => (x._2, x._1)).join(keySize).map(x=>(x._2._1,(x._1,x._2._2)))//(reads,(Ni,size))
    val cell=l1.join(k1).map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1,x._2.size))
    cell.take(100).foreach(println)

    /////////////////纯度
    val purity = cell.map(x => (x._1._1, x._2)).groupByKey().map {
      x => (x._1, x._2.max) //先找reads的max
    }.map(x => (x._1._1, x._2.toDouble / x._1._2.toDouble))
    //purity.take(100).foreach(println)
    val purityCount = purity.count()
    val puritySort = purity.map(x => (x._2.toString, x._1)).sortBy(_._1,false)
      .map(x => (x._1.toDouble, x._2))
    val purity_100 = purity.filter { x => x._2 == 1.0 }.count() / purityCount.toDouble
    val purity_median = {if (purityCount % 2 == 0){
       (puritySort.top((purityCount / 2).toInt).last._1 +
        puritySort.top((purityCount / 2 + 1).toInt).last._1) / 2
    }else{
      puritySort.top(((purityCount + 1) / 2).toInt).last._1}}
    val purity_95 = purity.filter { x => x._2 >=config.purity  }.count() / purityCount.toDouble
    /////////////////完整度
    val completeness = cell.map(x => (x._1._2, x._2)).groupByKey().map {
      x => (x._1, x._2.max)
    }.map(x => (x._1._1, x._2.toDouble / x._1._2.toDouble))
    // completeness.take(100).foreach(println)
    val completenessCount = completeness.count
    val completenessSort = completeness.map(x => (x._2.toString, x._1)).sortBy(_._1,false)
      .map(x => (x._1.toDouble, x._2))
    val completeness_100 = completeness.filter { x => x._2 == 1.0 }.count() / completenessCount.toDouble
    val completeness_median = {if (completenessCount % 2 == 0){
       (completenessSort.top((completenessCount / 2).toInt).last._1 +
        completenessSort.top((completenessCount / 2 + 1).toInt).last._1) / 2
    }else{
      completenessSort.top(((completenessCount + 1) / 2).toInt).last._1}}
    val completeness_80 = completeness.filter { x => x._2 >= config.completeness }.count()/ completenessCount.toDouble

    logInfo(s"############## Percent of 100% purity = ${purity_100 * 100}%")
    logInfo(s"############## Percent of 100% completeness = ${completeness_100 * 100}%")
    logInfo(s"############## Median purity = ${purity_median * 100}%")
    logInfo(s"############## Median completeness = ${completeness_median * 100}%")
    logInfo(s"############## 95% purity = ${purity_95 * 100}%")
    logInfo(s"############## 80% completeness = ${completeness_80 * 100}%")

    val res_list = List(
      "Percent of 100% purity" + "\t" + purity_100.formatted("%.4f"),
      "Percent of 100% completeness" + "\t" + completeness_100.formatted("%.4f"),
      "Median purity" + "\t" + purity_median.formatted("%.4f"),
      "Median completeness" + "\t" + completeness_median.formatted("%.4f"),
      "95% purity" + "\t" + purity_95.formatted("%.4f"),
      "80% completeness" + "\t" + completeness_80.formatted("%.4f"))
    val rdd_final_result = sc.parallelize(res_list,1)
    KmerCounting.delete_hdfs_file(config.output)
    rdd_final_result.saveAsTextFile(config.output)
/*
    val str = ">"
    val lpa = sc.textFile(config.lpa_file, minPartitions = config.n_partition)
      .map{line => line.split(str)}.map { x =>  x(1) }
      .map { line => line.split("\t|\n")}.map { x => (x(0), x(1)) }
    val key = sc.textFile(config.key_file, minPartitions = config.n_partition)
      .map { line => line.split("\t|\n")}.map { x => (x(0), x(1)) }
    val joinrdd=lpa.join(key)
    //joinrdd.take(100).foreach(println)
    val TP = joinrdd.map(u => (u._2, u._1)).groupByKey().map { //[（13，Ni），compact(ID1,ID2,....)]
      u => (u._1._1, u._1._2, u._2.size.toLong)
    }.filter(u => (u._3 >= 2)).map((x => (x._3 * (x._3 - 1)) / 2)).sum
    println("TP = "+TP)
    val FP = joinrdd.map(x => (x._2._1, x._1)).groupByKey().map {
      x => (x._1, x._2.size.toLong)
    }.filter(x => x._2 >= 2).map((x => (x._2 * (x._2 - 1)) / 2)).sum - TP
    println("FP = "+FP)
    val FN = joinrdd.map(x => (x._2._2, x._1)).groupByKey().map {
      x => (x._1, x._2.size.toLong)
    }.filter(x => x._2 >= 2).map((x => (x._2 * (x._2 - 1) / 2))).sum - TP
    println("FN = "+FN)
    val P = TP.toDouble / (TP + FP) //Precision
    val R = TP.toDouble / (TP + FN) //Recall
    val F1 = 2 * P * R / (P + R)
    val F5 = 26 * P * R / (25 * P + R)

    logInfo(s"############## P = $P")
    logInfo(s"############## R = $R")
    logInfo(s"##############F1 = $F1")
    logInfo(s"##############F5 = $F5")
*/
  }


  override def main(args: Array[String]) {
    val APPNAME = "Spark Metric"

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logInfo(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName(APPNAME).set("spark.kryoserializer.buffer.max", "512m")
        conf.registerKryoClasses(Array(classOf[DNASeq]))

        val sc = new SparkContext(conf)
        run(config, sc)
        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        sc.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main


}















