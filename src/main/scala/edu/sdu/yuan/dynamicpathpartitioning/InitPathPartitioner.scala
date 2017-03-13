package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.control.Breaks._

object InitPathPartitioner extends Serializable{
  def initializePPP(ppp: PathPartitioningPlan, alpha: Double = 0.8, iterNum: Int = 5): Unit = {
    var weights: RDD[(Int, Double)] = 
      VertexWeighting.vertexWeighting(ppp.vertices,
          ppp.edges, alpha, iterNum).map{case (id, (w1, w2)) => (id, w1 * w2)}.cache()

    val wc = weights.join(ppp.classes).cache()
    
    val sortedList = wc.map(t => (t._2._2, (t._2._1, 1)))
    .reduceByKey((a, b) => (a._1+b._1, a._2+b._2)).mapValues(t => t._1/t._2)
    .join(wc.map(t => (t._2._2, t._1)))
    .map(t => (t._2._2, (t._2._1, t._1))).join(ppp.vS)
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
        //println(t._1, result)
      }
    })
    
    sortedList.foreach( t => {
      if (t._2.size == 1) {
        val result = ppp.merger.merge(t._2.toSeq)
        if (result) {
          ppp.mergedVertice += ((t._1._2, t._1._1))
          ppp.mergedVerticeNum += 1
        }
      }
    })
    
    ppp.merger.assignPartition()

    val nodePartition: Map[Int, Int] = ppp.merger.nodePartition
    val np = ppp.sc.broadcast(nodePartition)
    
    ppp.nodePartition = ppp.vS.mapValues(set => {
      set.map { st => np.value.get(st).get }
    }).partitionBy(ppp.vS.partitioner.get).cache()
    
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
    .flatMap( t => t._2._2.map { p => (np.value.get(p).get, (t._1, t._2._1._1, t._2._1._2)) }.toSeq)
    .partitionBy(new HashPartitioner(ppp.partitionNum)).cache()
    ppp.dataMovement = ppp.result.count()
  }
}