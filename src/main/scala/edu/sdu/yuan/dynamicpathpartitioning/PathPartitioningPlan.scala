package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.broadcast._

class PathPartitioningPlan(sparkContext: SparkContext, dataFeeder: DataFeeder, partnum: Int, numExecutor: Int, numCore: Int, batchNumber: Int = -1) {
  val sc = sparkContext
  val feeder = dataFeeder
  val partitionNum = partnum
  val numCores = numCore
  val numExecutors = numExecutor
  var mergedVerticeNum: Long = 0
  var mergedClassesNum: Long = 0
  var mergedClasses: Set[(Int, Double)] = Set[(Int, Double)]()
  var mergedVertice: Set[(Int, Double)] = Set[(Int, Double)]()
  var dataMovement: Long = 0
  var triples: RDD[(Int, (Int, Int))] = feeder.getInitialInput(batchNumber)
  .setName("init_triple")
  .persist(StorageLevel.MEMORY_AND_DISK)

  var edges: RDD[(Int, Int)] = triples.filter(t => t._2._1 != 0 && t._2._2 != 37)
  .map(triple => (triple._1, triple._2._2))
  .partitionBy(triples.partitioner.get)
  .setName("init_edge")
  .persist(StorageLevel.MEMORY_AND_DISK)
  
  var vertice: RDD[(Int, Boolean)] = edges.flatMap{
      case (from, to) => Seq((to, false), (from, true))
    }.reduceByKey{case (a, b) => a && b}
  .setName("init_vertice")
  .persist(StorageLevel.MEMORY_AND_DISK)

  val classes: RDD[(Int, Int)] = feeder.getClasses()
  .setName("classes")
  .persist(StorageLevel.MEMORY_AND_DISK)
  
  val graph = Graph()
  
  val generator: StartingVertexGenerator = new StartingVertexGenerator()

  var vS: RDD[(Int, Set[Int])] = generator.generateStartingVertex(vertice, edges)
  .setName("init_vS")
  .persist(StorageLevel.MEMORY_AND_DISK)
  println("init_vS")
  val merger: PathGroupMerger = new PathGroupMerger(numExecutor)
  merger.extendTo(1+vertice.keys.reduce((a, b) => Math.max(a, b)))
  merger.setStartingNum(vertice.filter(t => t._2).count().toInt)
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
  
  def printN3LocalFiles(ppp: PathPartitioningPlan, feeder: DataFeeder, localname: String)
  : Unit = {
    val dict = ppp.sc.broadcast(feeder.getDictionary())
    val n3file: RDD[String] = ppp.result.map(tuple => {
      (dict.value.get(tuple._2._1).get + " " + dict.value.get(tuple._2._2).get 
          + " " + dict.value.get(tuple._2._3).get + " .")
    })
    println(localname)
    n3file.saveAsTextFile(localname)
  }
  
  
}