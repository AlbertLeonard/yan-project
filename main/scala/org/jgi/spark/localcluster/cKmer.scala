package org.jgi.spark.localcluster

import net.sparc.sparc

/**
  * Created by Lizhen Shi on 9/27/18.
  */
object cKmer {

  net.sparc.Info.load_native()


  // generates kmers
  def generate_kmer(seq: String, k: Int, is_canonical: Boolean=true): Array[DNASeq] = {
    val v = sparc.generate_kmer(seq, k, 'N', is_canonical)
    (0 until v.size().toInt).map(x=>DNASeq.from_bases(v.get(x))).toArray
  }

  def sequence_overlap(seq1: String, seq2: String, min_over_lap: Int, err_rate: Float) =
    sparc.sequence_overlap(seq1,seq2,min_over_lap,err_rate)

  def generate_edges(reads: Array[Int], max_degree: Int) = {
    val v = new net.sparc.intVector(reads.length)
    reads.indices.foreach(i=>v.set(i,reads(i)))
    val results = sparc.generate_edges(v, max_degree);
    (0 until results.size().toInt).map{
      i=>
        val a = results.get(i)
        (a.getFirst, a.getSecond)
    }
  }
}
