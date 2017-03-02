package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.broadcast._

class PathPartitioningPlan(sparkContext: SparkContext, dataFeeder: DataFeeder, partnum: Int, batchNumber: Int = -1) {
  val sc = sparkContext
  val feeder = dataFeeder
  val partitionNum = partnum
  
  var triples: RDD[(Int, (Int, Int))] = feeder.getInitialInput(batchNumber)
  .partitionBy(new HashPartitioner(partitionNum)).cache()
  var edges: RDD[(Int, Int)] = triples.map(triple => (triple._1, triple._2._2))
  .partitionBy(triples.partitioner.get).cache()
  
  var vertices: RDD[(Int, Boolean)] = edges.flatMap{
      case (from, to) => Seq((to, false), (from, true))
    }.reduceByKey{case (a, b) => a && b}
  .partitionBy(triples.partitioner.get).cache()

  val classes: RDD[(Int, Int)] = feeder.getClasses()
  .partitionBy(vertices.partitioner.get).cache()
  
  val generator: StartingVertexGenerator = new StartingVertexGenerator(vertices, edges)
  generator.generateStartingVertex()

  var vS: RDD[(Int, Set[Int])] = generator.getStartingVertex()
  val merger: PathGroupMerger = new PathGroupMerger(partitionNum, true)
  var nodePartition: RDD[(Int, Set[Int])] = null
  var result: RDD[(Int, (Int, Int, Int))] = null
  def loadBalance(): Seq[(Int, Int)] = result.mapPartitionsWithIndex(
      (index:Int, it) => 
        List((index, it.size)).iterator
      ).collect()
  def mergedSetSize(): (Long, Long) = {
      val merged = nodePartition.mapValues { x => x.size <= 1 }.cache()
      val mergedVertices = merged.filter(t => t._2).count()
      val mergedClasses = classes.join(merged).map{ case (_, (classno, merged)) => (classno, merged) }
      .reduceByKey((a, b) => a && b).filter(t => t._2).count()
      classes.join(merged).map{ case (_, (classno, merged)) => (classno, merged) }
      .reduceByKey((a, b) => a && b).filter(t => t._2).collect().foreach(println)
      (mergedVertices, mergedClasses)
    }
}

object PathPartitioningPlan extends Serializable{
  def printN3Files(ppp: PathPartitioningPlan, feeder: DataFeeder, outputname: String)
  : Unit = {
    val dict = ppp.sc.broadcast(feeder.getDictionary())
    val n3file: RDD[String] = ppp.result.map(tuple => {
      (dict.value.get(tuple._2._1).get + " " + dict.value.get(tuple._2._2).get 
          + " " + dict.value.get(tuple._2._3).get + " .")
    })
    n3file.saveAsTextFile(outputname)
  }
}