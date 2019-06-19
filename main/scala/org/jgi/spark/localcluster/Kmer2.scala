package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/13/17.
  */
object Kmer2 {

  // generates kmers hash values given the seq (doesn't handle 'N' and reverse complement)
  private def _generate_kmer(seq: String, k: Int, group_bits: String) = {
    (0 until (seq.length - k + 1)).map {
      i =>
        val subseq = seq.substring(i, i + k)
        if (group_bits == null || group_bits.isEmpty || subseq.startsWith(group_bits))
          DNASeq.from_bases(subseq)
        else
          null
    }.filter(_ != null)
  }

  // generates kmers
  def generate_kmer(seq: String, k: Int, group_bits: String): Array[DNASeq] = {
    seq.split("N").flatMap {
      subSeq => {
        _generate_kmer(subSeq, k, group_bits) ++ _generate_kmer(DNASeq.reverse_complement(subSeq), k, group_bits)
      }
    }.distinct
  }

  def generate_kmer(seq: String, k: Int): Array[DNASeq] = generate_kmer(seq, k, null)

  val GROUP4 = Array("A", "T", "C", "G")
  val GROUP16 = Array("AA", "AT", "AC", "AG", "TA", "TT", "TC", "TG", "CA", "CT", "CC", "CG", "GA", "GT", "GC", "GG")
  val GROUP64 = Array("AAA", "AAT", "AAC", "AAG", "ATA", "ATT", "ATC", "ATG", "ACA", "ACT", "ACC", "ACG", "AGA", "AGT", "AGC", "AGG", "TAA", "TAT", "TAC", "TAG", "TTA", "TTT", "TTC", "TTG", "TCA", "TCT", "TCC", "TCG", "TGA", "TGT", "TGC", "TGG", "CAA", "CAT", "CAC", "CAG", "CTA", "CTT", "CTC", "CTG", "CCA", "CCT", "CCC", "CCG", "CGA", "CGT", "CGC", "CGG", "GAA", "GAT", "GAC", "GAG", "GTA", "GTT", "GTC", "GTG", "GCA", "GCT", "GCC", "GCG", "GGA", "GGT", "GGC", "GGG")
  val GROUP256 = Array("AAAA", "AAAT", "AAAC", "AAAG", "AATA", "AATT", "AATC", "AATG", "AACA", "AACT", "AACC", "AACG", "AAGA", "AAGT", "AAGC", "AAGG", "ATAA", "ATAT", "ATAC", "ATAG", "ATTA", "ATTT", "ATTC", "ATTG", "ATCA", "ATCT", "ATCC", "ATCG", "ATGA", "ATGT", "ATGC", "ATGG", "ACAA", "ACAT", "ACAC", "ACAG", "ACTA", "ACTT", "ACTC", "ACTG", "ACCA", "ACCT", "ACCC", "ACCG", "ACGA", "ACGT", "ACGC", "ACGG", "AGAA", "AGAT", "AGAC", "AGAG", "AGTA", "AGTT", "AGTC", "AGTG", "AGCA", "AGCT", "AGCC", "AGCG", "AGGA", "AGGT", "AGGC", "AGGG", "TAAA", "TAAT", "TAAC", "TAAG", "TATA", "TATT", "TATC", "TATG", "TACA", "TACT", "TACC", "TACG", "TAGA", "TAGT", "TAGC", "TAGG", "TTAA", "TTAT", "TTAC", "TTAG", "TTTA", "TTTT", "TTTC", "TTTG", "TTCA", "TTCT", "TTCC", "TTCG", "TTGA", "TTGT", "TTGC", "TTGG", "TCAA", "TCAT", "TCAC", "TCAG", "TCTA", "TCTT", "TCTC", "TCTG", "TCCA", "TCCT", "TCCC", "TCCG", "TCGA", "TCGT", "TCGC", "TCGG", "TGAA", "TGAT", "TGAC", "TGAG", "TGTA", "TGTT", "TGTC", "TGTG", "TGCA", "TGCT", "TGCC", "TGCG", "TGGA", "TGGT", "TGGC", "TGGG", "CAAA", "CAAT", "CAAC", "CAAG", "CATA", "CATT", "CATC", "CATG", "CACA", "CACT", "CACC", "CACG", "CAGA", "CAGT", "CAGC", "CAGG", "CTAA", "CTAT", "CTAC", "CTAG", "CTTA", "CTTT", "CTTC", "CTTG", "CTCA", "CTCT", "CTCC", "CTCG", "CTGA", "CTGT", "CTGC", "CTGG", "CCAA", "CCAT", "CCAC", "CCAG", "CCTA", "CCTT", "CCTC", "CCTG", "CCCA", "CCCT", "CCCC", "CCCG", "CCGA", "CCGT", "CCGC", "CCGG", "CGAA", "CGAT", "CGAC", "CGAG", "CGTA", "CGTT", "CGTC", "CGTG", "CGCA", "CGCT", "CGCC", "CGCG", "CGGA", "CGGT", "CGGC", "CGGG", "GAAA", "GAAT", "GAAC", "GAAG", "GATA", "GATT", "GATC", "GATG", "GACA", "GACT", "GACC", "GACG", "GAGA", "GAGT", "GAGC", "GAGG", "GTAA", "GTAT", "GTAC", "GTAG", "GTTA", "GTTT", "GTTC", "GTTG", "GTCA", "GTCT", "GTCC", "GTCG", "GTGA", "GTGT", "GTGC", "GTGG", "GCAA", "GCAT", "GCAC", "GCAG", "GCTA", "GCTT", "GCTC", "GCTG", "GCCA", "GCCT", "GCCC", "GCCG", "GCGA", "GCGT", "GCGC", "GCGG", "GGAA", "GGAT", "GGAC", "GGAG", "GGTA", "GGTT", "GGTC", "GGTG", "GGCA", "GGCT", "GGCC", "GGCG", "GGGA", "GGGT", "GGGC", "GGGG")


}
