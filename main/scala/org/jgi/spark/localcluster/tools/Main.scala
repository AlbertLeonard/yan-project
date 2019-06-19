package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging

/**
  * Created by Lizhen Shi on 5/28/17.
  */
object Main extends LazyLogging {
  val apps: List[App] = List(KmerCounting, KmerMapReads2, GraphGen2, GraphMCL, GraphCC,GraphLPA2,GraphLPA3, CCAddSeq,
    Seq2Base64, Seq2Parquet, Repartition, SeqAddId,
    ClusterTNF, ClusterTNF2, GraphGenCluster, TNF, TNFCos, MergeClusters, MergeClusters2, GraphcosLPA, Metric, MeanShift, Test)
  val names: Map[String, App] = apps.map(x => (x.getClass.getSimpleName.replace("$", ""), x)).toMap

  def companion[T](implicit man: Manifest[T]): T =
    man.runtimeClass.getField("MODULE$").get(man.runtimeClass).asInstanceOf[T]

  def main(args: Array[String]) {
    if (args.length == 0 || (args.length == 1 && List("help", "-h", "--help").contains(args(0)))) {
      println("available apps (--help supported):")
      names.keys.foreach(x => println("\t" + x))
    } else {
      val cmd = args(0)
      if (!names.keys.toList.contains(cmd)) main(Array("--help"))
      else {
        val claz = names(cmd)
        claz.main(args.drop(1))
      }
    }
  }

  //main
}

