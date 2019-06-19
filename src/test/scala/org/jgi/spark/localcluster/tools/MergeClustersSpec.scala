package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

class MergeClustersSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext
{
  "MergeClusters" should "work on the test files with cluster_result" in {

    val cfg = MergeClusters.parse_command_line(("--cluster_file tmp/graph_lpa3_graphx.txt --re_cluster_file tmp/check_merge_coslpa.txt" +
      " -o tmp/2final_cluster_result --min_reads_per_cluster 2 -n 10 --n_output_blocks 1").split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")

    MergeClusters.run(cfg, sc)
  }
}

