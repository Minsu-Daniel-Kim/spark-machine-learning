import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Scored(md: MatchData, score: Double)
case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)
object index {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }
  def toDouble(s: String) = {
    if ("?".equals(s)) Double.NaN else s.toDouble
  }
  def parse(line: String) = {
    val pieces = line.split(',')
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
    MatchData(id1, id2, scores, matched)
  }
  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {

    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {

      val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))

      iter.foreach(arr => {

        nas.zip(arr).foreach { case (n, d) => n.add(d)}

      })
      Iterator(nas)
    })

    nastats.reduce((n1, n2) => {

      n1.zip(n2).map { case (a, b) => a.merge(b)}

    })

  }

  def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d




  def main(args : Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val rawblocks = sc.textFile("/Users/DanielMinsuKim/Documents/dataWarehouse/linkage")
    val head = rawblocks.take(100)
    val mds = head.filter(x => !isHeader(x)).map(x => parse(x))
    val noheader = rawblocks.filter(x => !isHeader(x))
    val parsed = noheader.map(line => parse(line)).cache()

//    val grouped = mds.map(md => md.matched).
//    grouped.mapValues(x => x.size).foreach(println)

    val matchCounts = parsed.map(md => md.matched).countByValue()

    val matchCountsSeq = matchCounts.toSeq


    val nas1 = Array(1.0, Double.NaN).map(d => NAStatCounter(d))
    val nas2 = Array(Double.NaN, 2.0).map(d => NAStatCounter(d))

    val nasRDD = parsed.map(md => {

      md.scores.map(d => NAStatCounter(d))

    })

    val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))

    statsm.zip(statsn).map { case(m, n) =>
      (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)

    val ct = parsed.map(md => {

      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)

    })

    ct.filter(s => s.score >= 4.0).map(s => s.md.matched).countByValue().foreach(println)



  }

}

