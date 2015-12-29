import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._

/**
  * Created by DanielMinsuKim on 12/27/15.
  */
object forestClassification {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = {

    val countsByCategory = data.map(_.label).countByValue()
    println(countsByCategory)
    val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)

  }


  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {

    val predictionsAndLabels = data.map(example =>

      (model.predict(example.features),example.label)

    )

    new MulticlassMetrics(predictionsAndLabels)

  }



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

//    val model = DecisionTree.trainClassifier(
//
//      trainData, 7, Map[Int, Int](), "gini", 4, 100
//    )
//
//    val model = RandomForest.trainClassifier(
//
//      trainData, 7, Map[Int, Int](), 20, "auto", "entropy", 30, 300
//
//    )

//    print(model)
//    val metrics = getMetrics(model, cvData)

    val trainPriorProbabilities = classProbabilities(trainData)

    val cvPriorPrbabilities = classProbabilities(cvData)

//    print(trainPriorProbabilities.zip(cvPriorPrbabilities).map {
//
//      case (trainProb, cvProb) => trainProb * cvProb
//
//    }.sum)

    val evaluations = for (impurity <- Array("gini", "entropy");
        depth <- Array(1, 20);
        bins  <- Array(10, 300))

      yield {
//        val model = DecisionTree.trainClassifier(
//
//          trainData, 7, Map[Int, Int](), impurity, depth, bins)
        val model = RandomForest.trainClassifier(

          trainData, 7, Map[Int, Int](), 20, "auto", "entropy", 30, 300

        )
        val predictionsAndLabels = cvData.map(example =>
          (model.predict(example.features), example.label)

        )

        val accuracy =
        new MulticlassMetrics(predictionsAndLabels).precision
        ((impurity, depth, bins), accuracy)
      }
    print(evaluations.sortBy(_._2).reverse.foreach(println))

  }
}
