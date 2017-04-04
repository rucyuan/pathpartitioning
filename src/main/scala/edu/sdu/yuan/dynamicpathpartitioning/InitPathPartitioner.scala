package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.control.Breaks._

object InitPathPartitioner extends Serializable{
  def initializePPP(ppp: PathPartitioningPlan, alpha: Double = 0.8, iterNum: Int = 5): Unit = {
    var weights: RDD[(Int, Double)] = 
      VertexWeighting.vertexWeighting(ppp.vertice,
          ppp.edges, alpha, iterNum).mapValues{case (w1, w2) => (w1 * w2)}
    .setName("weights")

    val wc = weights.join(ppp.classes)
    .setName("wc")
    .persist(StorageLevel.MEMORY_AND_DISK)
    
    val sorted = wc.map(t => (t._2._2, (t._2._1, 1)))
    .reduceByKey((a, b) => (a._1+b._1, a._2+b._2)).mapValues(t => t._1/t._2)
    .join(wc.map(t => (t._2._2, t._1)))
    .map(t => (t._2._2, (t._2._1, t._1))).join(ppp.vS)
    .map(t => (t._2._1, (t._1, t._2._2)))
    .sortByKey(true, ppp.partitionNum)
    .setName("sorted")
    .persist(StorageLevel.MEMORY_AND_DISK)
    println(sorted.count())
    sorted.saveAsTextFile("sortResult")
    
    /*sorted.mapPartitionsWithIndex((pid, iter) => iter.map(t => (pid, t._1)))
    .collect().foreach(println)*/
    
    sorted.mapPartitionsWithIndex((pid, iter) => Array((pid, iter.size)).iterator)
    .collect().foreach(println)
    
    val classList = sorted.filter(t => t._1._2 != t._2._1)
    .setName("classList")
    
    val literalList = sorted.filter(t => t._1._2 == t._2._1)
    .setName("literalList")
    
    val classParts = classList.partitions
    val literalParts = literalList.partitions
    
    var last: Int = -1
    var lw: Double = 0
    var successful: Boolean = false

    for (p <- classParts) {
      val idx = p.index
      val partRdd = classList.mapPartitionsWithIndex((p, iter) => if (p == idx) iter else Iterator())
      val data = partRdd.collect.toList
      data.foreach{ case ((weight, classid), (nid, set)) => {
        if (last != classid) {
          println(last, successful)
          if (successful) {
            ppp.mergedClasses += ((last, lw))
          }
          ppp.merger.backup()
          last = classid
          lw = weight
          successful = true
        }
        
        if (successful) successful = ppp.merger.merge(set, false)
        else {
          ppp.merger.setStartingVertex(set)
        }
      }}
    }
    println(last, successful)
    if (successful) {
          ppp.mergedClasses += ((last, lw))
        }
    
    for (p <- literalParts) {
      val idx = p.index
      val partRdd = literalList.mapPartitionsWithIndex((p, iter) => if (p == idx) iter else Iterator())
      val data = partRdd.collect.toList
      data.foreach{ case ((weight, classid), (nid, set)) => {
        ppp.merger.backup()
        successful = ppp.merger.merge(set, false)
        if (successful) {
          ppp.mergedVertice += ((classid, weight))
        }
      }}
    }
    /*val classIter = classList.toLocalIterator
    val literalIter = literalList.toLocalIterator
    var last: Int = -1
    var lw: Double = 0
    var successful: Boolean = false
    while (classIter.hasNext) {
      val ((weight, classid), (nid, set)) = classIter.next()
      if (last != classid) {
        if (successful) {
          ppp.mergedClasses += ((last, lw))
          ppp.mergedClassesNum += 1
        }
        ppp.merger.backup()
        last = classid
        lw = weight
        successful = true
      }
      if (successful) ppp.merger.merge(set, false)
      else {
        ppp.merger.setStartingVertex(set)
      }
    }
    if (successful) {
          ppp.mergedClasses += ((last, lw))
          ppp.mergedClassesNum += 1
          successful = false
        }
    
    while (literalIter.hasNext) {
      val ((weight, classid), (nid, set)) = literalIter.next()
      if (last != classid) {
        if (successful) {
          ppp.mergedVertice += ((last, lw))
          ppp.mergedVerticeNum += 1
        }
        ppp.merger.backup()
        last = classid
        lw = weight
        successful = true
      }
      if (successful) ppp.merger.merge(set, false)
      else {
        ppp.merger.setStartingVertex(set)
      }
    }
    
    if (successful) {
          ppp.mergedVertice += ((last, lw))
          ppp.mergedVerticeNum += 1
        }
    */
    /*
    val sortedList = wc.map(t => (t._2._2, (t._2._1, 1)))
    .reduceByKey((a, b) => (a._1+b._1, a._2+b._2)).mapValues(t => t._1/t._2)
    .join(wc.map(t => (t._2._2, t._1)))
    .map(t => (t._2._2, (t._2._1, t._1))).join(ppp.vS.filter(t => t._2.size > 1))
    .map(t => (t._2._1, (t._1, t._2._2)))
    .groupByKey().collect().toList.sortWith((a, b) => a._1._1 < b._1._1)

    
    sortedList.foreach( t => {
      if (t._2.size > 1) {
        val result = ppp.merger.merge(t._2.toSeq)
        if (result) {
          ppp.mergedClasses += ((t._1._2, t._1._1))
          ppp.mergedClassesNum += 1
          ppp.mergedVerticeNum += t._2.size
        }
        println(t._1, result)
      }
    })
    
    sortedList.foreach( t => {
      if (t._2.size == 1) {
        if (t._2.head._2.size == 1) {
          ppp.mergedVertice += ((t._1._2, t._1._1))
          ppp.mergedVerticeNum += 1
        } else
        {
          val result = ppp.merger.merge(t._2.toSeq)
          if (result) {
            ppp.mergedVertice += ((t._1._2, t._1._1))
            ppp.mergedVerticeNum += 1
          }
        }
      }
    })*/
    
    ppp.merger.assignPartition()
    
    val nodePartition: Map[Int, Int] = ppp.merger.nodePartition
    val np = ppp.sc.broadcast(nodePartition)
    
    ppp.nodePartition = ppp.vS.mapValues(set => {
      set.map { st => np.value.get(st).get }
    }).partitionBy(ppp.vS.partitioner.get).setName("node_partition").persist(StorageLevel.MEMORY_AND_DISK)
    
    /*val t0 = System.nanoTime()
    ppp.vS.mapValues(set => {
      set.seq.map { st => np.value.get(st).get }.groupBy(t => t)
      .map(t => (t._1, t._2.size)).toSet
    }).count()
    val t1 = System.nanoTime()
    ppp.vS.mapValues(set => {
      set.map { st => np.value.get(st).get }.toSet
    }).count()
    val t2 = System.nanoTime()
    println((t1-t0)/1e9.toLong, (t2-t1)/1e9.toLong)
    */

    ppp.result = ppp.triples.join(ppp.vS)
    .flatMap( t => t._2._2.map { p =>(np.value.get(p).get, (t._1, t._2._1._1, t._2._1._2))
      }.toSeq)
    .partitionBy(new HashPartitioner(ppp.numExecutors))
    .setName("result").persist(StorageLevel.MEMORY_AND_DISK)

    ppp.dataMovement = ppp.result.count()
  }
}