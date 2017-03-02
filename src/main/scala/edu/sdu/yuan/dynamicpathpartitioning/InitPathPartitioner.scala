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
    
    val topkList = wc.map(t => (t._2._2, (t._2._1, 1)))
    .reduceByKey((a, b) => (a._1+b._1, a._2+b._2)).mapValues(t => t._1/t._2)
    .map(t => if (t._1 < 0) (t._1, Double.MaxValue) else (t._1, t._2))
    .collect().sortBy(t => t._2).take(8)
    
    topkList.foreach(println)
    
    weights = ppp.sc.parallelize(topkList)
    .join(wc.map(t => (t._2._2, t._1))).map(t => (t._2._2, t._2._1)).cache()
    
    val sortedList: List[(Int, (Set[Int], Double))] =  ppp.vS.join(weights).collect()
    .sortWith((left, right) => left._2._2 < right._2._2).toList
    
    ppp.nodePartition = ppp.sc.parallelize(ppp.merger.bottomUpMerging(sortedList)).cache()
    
    println(ppp.nodePartition.filter( t=> t._2.size <= 1).count())
    
    ppp.result = ppp.triples.join(ppp.nodePartition)
    .map{ case (s, ((p, o), set)) => {
      (o, ((s, set), p))
    }
    }.join(ppp.nodePartition).map{ case (o, (((s, setS), p), setO)) => {
      ((s, setS), (o, setO), p)
    }
    }
    .flatMap{ case ((s, setS), (o, setO), p) => {
      setS.intersect(setO).toSeq.map { ele => (ele, (s, p, o)) }
    }
    }.partitionBy(new HashPartitioner(ppp.partitionNum))
    
  }
}