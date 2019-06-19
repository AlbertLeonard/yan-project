package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class KmerMapReads2Spec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext
{

//  "parse command line" should "be good" in {
//    val cfg = KmerMapReads2.parse_command_line("--reads test -p *.seq -k 31 --kmer data/kmercounting_test.txt -o tmp".split(" ")).get
//    cfg.reads_input should be("test")
//  }
  /*
  "kmer mapping" should "work on the test seq files" in {
    val cfg = KmerMapReads2.parse_command_line(
      "--reads data/small -p sample.seq --kmer data/kmercounting_test.txt -k 31  -o tmp/kmermapping2_seq_test.txt --n_iteration 1".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    KmerMapReads2.run(cfg, sc)
    //Thread.sleep(1000 * 10000)
  }

  "kmer mapping" should "work on the test seq files with out top kmers" in {
    val cfg = KmerMapReads2.parse_command_line(
      "--reads data/small -p sample.seq --kmer data/kmercounting_test.txt -k 31 --contamination 0  -o tmp/kmermapping2_seq_test.txt --n_iteration 1".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    KmerMapReads2.run(cfg, sc)
    //Thread.sleep(1000 * 10000)
  }

  "kmer mapping" should "work on the test seq files with N>1" in {
    val cfg = KmerMapReads2.parse_command_line(
      "--reads data/small -p sample.seq --kmer data/kmercounting_test.txt -k 31  -o tmp/kmermapping2_seq_test2.txt --n_iteration 2".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    KmerMapReads2.run(cfg, sc)
    //Thread.sleep(1000 * 10000)
  }

  "kmer mapping" should "work on the test seq files with N>1 with native lib" in {
    val cfg = KmerMapReads2.parse_command_line(
      "--reads data/small -p sample.seq --kmer data/kmercounting_test.txt -k 31  -o tmp/kmermapping2_seq_test2_native.txt --n_iteration 2 --use_native".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    KmerMapReads2.run(cfg, sc)
    //Thread.sleep(1000 * 10000)
  }
  */
  "kmer mapping" should "work on the test seq files" in {
    val cfg = KmerMapReads2.parse_command_line(
      "--reads data/small -p sample.seq -c 0 -k 31  -o tmp/kmermapping2_seq_test.txt --n_iteration 1".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    KmerMapReads2.run(cfg, sc)
    //Thread.sleep(1000 * 10000)
  }
}
