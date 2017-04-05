package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.broadcast._
import org.apache.spark.graphx._
import org.apache.spark.rdd._

class PathPartitioningPlan(sparkContext: SparkContext, dataFeeder: DataFeeder, numPartition: Int, numExecutor: Int, numCore: Int, batchNumber: Int = -1) {
  val sc = sparkContext
  val feeder = dataFeeder
  val numPartitions = numPartition
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

  val edges: RDD[Edge[Null]] = triples.filter(t => t._2._1 != 0 && t._2._2 != 37)
  .map(triple => Edge(triple._1, triple._2._2))
  
  val vertices: RDD[(VertexId, Boolean)] = edges.flatMap{
      e => Seq((e.dstId, false), (e.srcId, true))
    }.reduceByKey{case (a, b) => a && b}
  
  val initialGraph = Graph(vertices, edges, false, StorageLevel.MEMORY_AND_DISK, 
      StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.EdgePartition2D, numPartitions)
  
  val sGraph = initialGraph.mapVertices((id, starting) => 
        if (starting) (true, Set[VertexId](id), id) 
        else (false, Set[VertexId](), id))
  
  
  val startingGraph = sGraph.pregel(Gen1.initialMsg, Int.MaxValue,
  EdgeDirection.Out)(Gen1.vprog, Gen1.sendMsg, Gen1.mergeMsg)
  .mapVertices((id, attr) => 
    if (attr._2.isEmpty) (true, Set[VertexId](attr._3))
    else (false, attr._2)
  ).pregel(Gen2.initialMsg, Int.MaxValue, EdgeDirection.Out
      )(Gen2.vprog, Gen2.sendMsg, Gen2.mergeMsg)
  .persist(StorageLevel.MEMORY_AND_DISK)
  
  val wGraph = initialGraph.mapVertices((id, _) => (1.0, 1.0))
  
  val weightedGraph = wGraph.pregel(Wei.initialMsg, 5, EdgeDirection.Out)(
      Wei.vprog, Wei.sendMsg, Wei.mergeMsg)
      .mapVertices{case (id, (in, out)) => in * out}
  .persist(StorageLevel.MEMORY_AND_DISK)

  val classes: RDD[(VertexId, VertexId)] = feeder.getClasses()
  .persist(StorageLevel.MEMORY_AND_DISK)
  
  val classRelationship: RDD[Edge[Null]] = 
    classes.map{ case (id, cid) => Edge(id, cid) }
  
  val sortedClassList = Graph(weightedGraph.vertices, classRelationship)
  .aggregateMessages[(Int, Double)](
      triplet => triplet.sendToDst(1, triplet.srcAttr), 
      (a, b) => (a._1 + b._1, a._2 + b._2)
      )
  .mapValues( (id, value) => value match {
    case (count, totalWeight) => totalWeight / count
  }).sortBy(t => t._2).join(classes.join(startingGraph.vertices).map{
    case (vid, (cid, (_, set))) => (cid, (vid, set))
  })
  .persist(StorageLevel.MEMORY_AND_DISK)
  
  sortedClassList.mapPartitionsWithIndex((pid, iter) => Array((pid, iter.size)).iterator)
  .collect().foreach(println)
  
  val merger: PathGroupMerger = new PathGroupMerger(numExecutor)
  //merger.extendTo(1+(vertices.keys.reduce((a, b) => Math.max(a, b)).toLong))
  merger.setStartingNum(vertices.filter(t => t._2).count().toInt)
  var nodePartition: RDD[(Int, Set[Int])] = null
  var result: RDD[(Int, (Int, Int, Int))] = null
  def loadBalance(): Seq[(Int, Int)] = result.mapPartitionsWithIndex(
      (index:Int, it) => 
        List((index, it.size)).iterator
      ).collect()
  def mergedSetSize(): (Long, Long) = {
      val merged = nodePartition.mapValues { x => x.size <= 1 }.cache()
      val mergedVertices = merged.filter(t => t._2).count()
      /*val mergedClasses = classRelationship.join(merged).map{ case (_, (classno, merged)) => (classno, merged) }
      .reduceByKey((a, b) => a && b).filter(t => t._2).count()
      classes.join(merged).map{ case (_, (classno, merged)) => (classno, merged) }
      .reduceByKey((a, b) => a && b).filter(t => t._2).collect().foreach(println)
      (mergedVertices, mergedClasses)*/
      (0, 0)
    }
}

object Gen1 {
  val initialMsg: (Set[VertexId], VertexId) = (Set[VertexId](), Long.MaxValue)

  def vprog(vid: VertexId, value: (Boolean, Set[VertexId], VertexId), message: (Set[VertexId], VertexId))
  :(Boolean, Set[VertexId], VertexId) = {
    if (message._2.equals(Long.MaxValue)) { 
      value
    } else {
      val set = value._2.union(message._1)
      val min = Math.min(value._3, message._2)
      val changed = if (set.equals(value._2) && min == value._3) false;
      else true;
      (changed, set, min)
    }
  }
  
  def sendMsg(triplet: EdgeTriplet[(Boolean, Set[VertexId], VertexId), Null])
  :Iterator[(VertexId, (Set[VertexId], VertexId))] = {
    val attr = triplet.srcAttr
    println(attr)
    if (attr._1) Iterator((triplet.dstId, (attr._2, attr._3)))
    else Iterator.empty
  }
  
  def mergeMsg(msg1: (Set[VertexId], VertexId), msg2: (Set[VertexId], VertexId))
  :(Set[VertexId], VertexId) = {
    (msg1._1.union(msg2._1), Math.min(msg1._2, msg2._2))
  }
}

object Gen2 {
  val initialMsg: Set[VertexId] = Set[VertexId]()

  def vprog(vid: VertexId, value: (Boolean, Set[VertexId]), message: Set[VertexId])
  :(Boolean, Set[VertexId]) = {
    if (message.isEmpty) { value }
    else {
      val set = value._2.union(message)
      val changed = if (set.equals(value._2)) false;
      else true;
      (changed, set)
    }
  }
  
  def sendMsg(triplet: EdgeTriplet[(Boolean, Set[VertexId]), Null])
  :Iterator[(VertexId, Set[VertexId])] = {
    val attr = triplet.srcAttr
    if (attr._1) Iterator((triplet.dstId, attr._2))
    else Iterator.empty
  }
  
  def mergeMsg(msg1: Set[VertexId], msg2: Set[VertexId])
  :(Set[VertexId]) = {
    (msg1.union(msg2))
  }
}

object Wei {
  val initialMsg: (Double, Double, Double, Double) = (0.0, 0.0, 0.0, 0.0)
  val alpha: Double = 0.8
  def vprog(vid: VertexId, value: (Double, Double), message: (Double, Double, Double, Double))
  :(Double, Double) = {
    val in = if (message._2 == 0) value._1 else (1-alpha)+alpha * message._1/Math.sqrt(message._2)
    val out = if (message._4 == 0) value._2 else (1-alpha)+alpha * message._3/Math.sqrt(message._4)
    (in, out)
  }
  
  def sendMsg(triplet: EdgeTriplet[(Double, Double), Null])
  :Iterator[(VertexId, (Double, Double, Double, Double))] = {
    val attr = triplet.srcAttr
    Iterator(
        (triplet.dstId, (attr._1, attr._1 * attr._1, 0, 0)), 
        (triplet.srcId, (0, 0, attr._2, attr._2 * attr._2))
        )
  }
  
  def mergeMsg(msg1: (Double, Double, Double, Double), msg2: (Double, Double, Double, Double))
  :(Double, Double, Double, Double) = {
    (msg1._1+msg2._1, msg1._2+msg2._2, msg1._3+msg2._3, msg1._4+msg2._4)
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