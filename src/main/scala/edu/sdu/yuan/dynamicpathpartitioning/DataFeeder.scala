package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage._

class DataFeeder(sc: SparkContext, insertionRatio: Double, deletionRatio: Double, num: Int, inputname: String, dictionaryname: String, partitionNum: Int) {
  val lines: RDD[String] = sc.textFile(inputname, partitionNum)
  val triples: RDD[(Int, (Int, Int))] = 
    lines.map(line => (line.split(" ")(0).toInt, (line.split(" ")(1).toInt, line.split(" ")(2).toInt)))
    .partitionBy(new HashPartitioner(partitionNum)).setName("input")
  var updatetimes: Int = -1
  var addition: RDD[(Int, (Int, Int))] = 
    triples.sample(false, insertionRatio * num, 0)
  val initial: RDD[(Int, (Int, Int))] = triples.subtract(addition)
    .partitionBy(triples.partitioner.get).persist(StorageLevel.MEMORY_AND_DISK)

  var deletion: RDD[(Int, (Int, Int))] = 
    initial.sample(false, (deletionRatio * num)/(1-insertionRatio * num), 0)
    .partitionBy(initial.partitioner.get).persist(StorageLevel.MEMORY_AND_DISK)
  
  var delta: Array[RDD[(Boolean, (Int, Int, Int))]] = Array[RDD[(Boolean, (Int, Int, Int))]]()
  
  for (k <- Range(0, num)) {
    val a = addition.sample(false, 1.0/(num - k), 0)
    .partitionBy(addition.partitioner.get).persist(StorageLevel.MEMORY_AND_DISK)
    val d = deletion.sample(false, 1.0/(num - k), 0)
    .partitionBy(deletion.partitioner.get).persist(StorageLevel.MEMORY_AND_DISK)
    addition = addition.subtract(a)
    .partitionBy(addition.partitioner.get).persist(StorageLevel.MEMORY_AND_DISK)
    deletion = deletion.subtract(d)
    .partitionBy(deletion.partitioner.get).persist(StorageLevel.MEMORY_AND_DISK)
    delta ++= Array( a.map(t => (true, (t._1, t._2._1, t._2._2)))
        .union(d.map(t => (false, (t._1, t._2._1, t._2._2)))) )
  }

  val entities: RDD[(Int, (String, Int))] =  sc.textFile(dictionaryname, partitionNum)
    .map ( line => (line.split(" ")(0).toInt, (line.split(" ")(1),
        line.split(" ")(2).toInt )))
        .partitionBy(new HashPartitioner(partitionNum))
        .setName("entities")
        //.persist(StorageLevel.MEMORY_AND_DISK)
        
  def getInitialInput(batchNumber: Int): RDD[(Int, (Int, Int))] = {
    if (batchNumber < 0) initial
    else {
      var out = initial
      for (x <- Range(0, batchNumber+1)) {
        out = out.subtract(delta(x).filter(t => !t._1).map(t => (t._2._1, (t._2._2, t._2._3))))
        .partitionBy(out.partitioner.get).union(delta(x)
        .partitionBy(out.partitioner.get).filter(t => t._1).map(t => (t._2._1, (t._2._2, t._2._3))))
        .partitionBy(out.partitioner.get)
      }
      out
    }
  }
  
  def getNextDeltaInput(): RDD[(Boolean, (Int, Int, Int))] = {
    updatetimes += 1
    if (updatetimes >= num) sc.emptyRDD
    else  delta(updatetimes)
  }
  
  def getDictionary(): Map[Int, String] = {
    entities.map{ case (id, (uri, attr)) => { 
      if (attr == 0) {
        (id, "<" + uri + ">")
        } else {
          (id, "\"" + uri + "\"")
          }
      }
    }.collectAsMap().toMap
  }
  def getClasses(): RDD[(Int, Int)] = {
    val literal = entities.filter(t => t._2._2 == 1).map( t => (t._1, t._1))
    .partitionBy(entities.partitioner.get)
    triples.filter(t => t._2._1 == 0).map(t => (t._1, t._2._2)).union(literal)
    .partitionBy(triples.partitioner.get)
    .rightOuterJoin(entities.keyBy(t => t._1)).flatMap(t => {
      if (t._2._1.isEmpty) Seq()
      else Seq((t._1, t._2._1.get))
    }).partitionBy(triples.partitioner.get)
  }
}