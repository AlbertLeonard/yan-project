/**
  * Created by Yakang Lu on 23/11/18.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import org.jgi.spark.localcluster.{DNASeq, Utils}
import scala.collection.mutable.{Set,Map}
import sext._
import scala.collection.mutable.Set  //added at 2018.10.08


object Evaluate extends App with LazyLogging {

  case class Config(lpa_file: String = "", key_file: String = "", output: String = "")

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("PurityAndCompleteness3") {
      head("PurityAndCompleteness3", Utils.VERSION)

      opt[String]('l', "lpa_file").required().valueName("<file>").action((x, c) =>
        c.copy(lpa_file = x)).text("input files of example_lpa")

      opt[String]('k', "key_file").required().valueName("<file>").action((x, c) =>
        c.copy(key_file = x)).text("input files of example_key")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")
      //opt[Int]('n', "n_start").action((x, c) => c.copy(n_start = x)).text("start flag")

      help("help").text("prints this usage text")
    }
    parser.parse(args, Config())
  }

  def logInfo(str: String) = {
    logger.info(str)
    println("AAAA " + str)
  }

  // run function
  def run(config: Config, spark: SparkSession): Unit = {

    val sc = spark.sparkContext
    sc.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")


    val rdd1_new = sc.textFile(config.lpa_file).map(x =>
      (x.split("\t| ").filter(_.length>0)(0), x.split("\t| | ").filter(_.length>0)(1)))
    //rdd1_new.foreach(println(_))
    //println("###")

    //再次利用正则表达式，利用TAB分割字符串，map映射形成（0, 14276:7755）格式的有用数据。
    val rdd2_new = sc.textFile(config.key_file).map(x =>
      (">"+x.split("\t| ").filter(_.length>0)(0), x.split("\t| | ").filter(_.length>0)(1)))
    //rdd2_new.foreach(println(_))

    val rdd_join = rdd1_new.join(rdd2_new)
    //rdd_join.take(10).foreach(println(_))
    //rdd_co.foreach(println(_))
    /*
    val rdd_addnum = rdd_join.map(x => ((x._2._1.head,x._2._2.head),1))
    //rdd_addnum.foreach(println(_))
    val rdd_redu = rdd_addnum.reduceByKey((a, b) => a + b).sortBy(_._1._2).sortBy(_._1._1)
    //rdd_redu.foreach(println(_))
    */

    //非本地化的遍历rdd变量操作！！
    val arr_c1 = rdd_join.groupBy(_._2._2).filter(_._2.toList.length>2).sortBy(_._1).collect()
    val arr_p2 = rdd_join.groupBy(_._2._1).sortBy(_._1).collect()
    //arr_p1.foreach(println(_))
    val rdd2_new2 = rdd2_new.map(x => (x._1,x._2.split("-")(0)))
    val rdd_join2 = rdd1_new.join(rdd2_new2)
    //rdd_join2.foreach(println(_))
    val arr_p1 = rdd_join2.groupBy(_._2._1).filter(_._2.toList.length>=2).sortBy(_._1).collect()
    val arr_c2 = rdd_join2.groupBy(_._2._2).sortBy(_._1).collect()
    //arr_c2.foreach(println(_))

    //val ppp = rdd_redu.lookup((3.toString, 3.toString) //找不到会返回empty
    //val redu_arr = rdd_redu.collect()

    //at column, calculate purity
    var purity_map: Map[String,Double] = Map()
    for (i <- arr_p1){
      var num_set: Set[Int] = Set()
      for (j <- arr_c2) {
        num_set.add(i._2.toList.intersect(j._2.toList).length)
      }
      //println(num_set)
      val col_max = num_set.max   //求出每列最大值
      purity_map += (i._1 -> col_max*1.0/i._2.toList.length)
    }
    //purity_map.foreach(println(_))   //...

    //at raw, calculate completeness
    var comple_map: Map[String,Double] = Map()
    for (j <- arr_c1){
      var num_set2: Set[Int] = Set()
      for (i <- arr_p2) {
        num_set2.add(i._2.toList.intersect(j._2.toList).length)
      }
      //println(num_set)
      val raw_max = num_set2.max   //求出每行最大值
      comple_map += (j._1 -> raw_max*1.0/j._2.toList.length)
    }
    //comple_map.foreach(println)   //...


    //求纯度、完整度中位数：
    val purity_sort_list = purity_map.values.toList.sortWith(_ < _)
    val comple_sort_list = comple_map.values.toList.sorted
    val count_p = purity_sort_list.length
    val count_c = comple_sort_list.length
    //println(purity_sort_list)
    println("count of p&c: "+ count_p,count_c)
    //purity_median
    var medp:Double = -0.0
    if (count_p%2 == 1){
      medp = purity_sort_list((count_p+1)/2-1)
    }
    else if(count_p > 0){
      medp = (purity_sort_list(count_p/2-1)+purity_sort_list(count_p/2))/2
    }
    else{ medp = 0.0}   //add 02.12.2018
    //completeness_median
    var medc:Double = -0.0
    if (count_c%2 == 1){
      medc = comple_sort_list((count_c+1)/2-1)
    }
    else if(count_c > 0){
      medc = (comple_sort_list(count_c/2-1)+comple_sort_list(count_c/2))/2
    }
    else{ medc = 0.0 }    //add 02.12.2018
    //输出纯度、完整度中位数：
    //println((medp.formatted("%.4f"), medc.formatted("%.4f")))

    //求纯度、完整度各自100%所占的比例：
    val rdd_p = sc.parallelize(purity_map.toList,1)
    val rdd_p_result = rdd_p.map(x => (x._1,x._2)) //不去重，因为可能会有纯度完整度相同的。
    val rdd_c = sc.parallelize(comple_map.toList,1)
    val rdd_c_result = rdd_c.map(x => (x._1,x._2))
    //calculate
    val ratep_100 = rdd_p_result.filter(_._2.toInt == 1).count()*1.0/rdd_p_result.count()
    val ratec_100 = rdd_c_result.filter(_._2.toInt == 1).count()*1.0/rdd_c_result.count()


    val res_list = List("P_med:  "+medp.formatted("%.4f"), "P_100%: "+ratep_100.formatted("%.4f"),
      "C_med:  "+medc.formatted("%.4f"), "C_100%: "+ratec_100.formatted("%.4f"))
    val rdd_final_result = sc.parallelize(res_list,1)

    //打印输出和存为本地txt
    println("Median & 100%_rate: ")
    rdd_final_result.collect().foreach(println(_))
    KmerCounting.delete_hdfs_file(config.output)  //delete already exist output file
    rdd_final_result.saveAsTextFile(config.output)

  }


  override def main(args: Array[String]) {
    val APPNAME = "PurityAndCompleteness3"

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logInfo(s"called with arguments\n${options.valueTreeString}")
        //require(config.n_start > 0)

        val conf = new SparkConf().setAppName(APPNAME)

        conf.registerKryoClasses(Array(classOf[DNASeq]))
        val spark = SparkSession.builder().config(conf).appName(APPNAME).getOrCreate()

        run(config, spark)
        spark.stop()

      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}

