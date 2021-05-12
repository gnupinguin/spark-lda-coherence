package io.gnupinguin.lda.coherence

import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField}
import org.apache.spark.sql.{Column, Dataset, Row, RowFactory, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.List

class CoherenceTest extends AnyFunSuite{

  test("Test Coherence with basic NLP") {

    val spark: SparkSession = SparkSession.builder.appName("Local application")
      .master("local[*]").getOrCreate

    val tokenizer: Tokenizer = new Tokenizer
    tokenizer.setInputCol("textContent")
    tokenizer.setOutputCol("tokens")

    val stopWordsRemover: StopWordsRemover = new StopWordsRemover
    stopWordsRemover.setInputCol("tokens")
    stopWordsRemover.setOutputCol("filtered")

    val countVectorizer: CountVectorizer = new CountVectorizer
    countVectorizer.setInputCol("filtered")
    countVectorizer.setOutputCol("tf")

    var data: Dataset[Row] = loadData(spark)
    data = stopWordsRemover.transform(tokenizer.transform(data))

    val vectorizerModel: CountVectorizerModel = countVectorizer.fit(data)
    data = vectorizerModel.transform(data)

    val ldaModel: LDAModel = getLda(3).fit(data)
    val topics: Dataset[Row] = ldaModel.describeTopics(4)

    //topic coherence
    val coherenceModel: UMassCoherenceModel = new UMassCoherenceModel("tf", data.select("tf"))
    val topicsCoherence: Dataset[Row] = coherenceModel.transform(topics)
    topicsCoherence
      .select(col("topic"), topicTerms(vectorizerModel.vocabulary, "termIndices").as("terms"), col("topicCoherence"))
      .show(false)

    //average topic coherence
    topicsCoherence.select("topicCoherence")
      .agg(avg("topicCoherence")).show()

    spark.stop()
  }

  def topicTerms(vocabulary: Array[String], termIndicesCol: String): Column = {
    udf((ind: Array[Int] ) => ind.map(i => vocabulary(i)))
      .apply(col(termIndicesCol))
  }


  private def getLda(k: Int): LDA = {
    val lda: LDA = new LDA
    lda.setOptimizer("online").setK(k).setMaxIter(10).setSeed(123).setFeaturesCol("tf").setTopicDistributionCol("topicDistribution")
    lda
  }

  private def loadData(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    Seq(
      "the cat meows on the dog",
      "the dog barked at the cat",
      "the dog barked at the chicken",
      "the dog founds the bone",
      "the cat eats mouse",
      "the chicken eats bread",
    ).toDF("textContent")
  }

}
