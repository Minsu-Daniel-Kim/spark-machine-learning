import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg._

/**
  * Created by DanielMinsuKim on 12/27/15.
  */
object forestClassification {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args : Array[String]): Unit = {

    val conf = new SparkConf().setAppName("forest classification").setMaster("local")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile("/Users/DanielMinsuKim/Documents/dataWarehouse/forests")

    val data = rawData.map { line =>

      val values = line.split(",").map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)

    }

    val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    print(trainData)



  }
}
