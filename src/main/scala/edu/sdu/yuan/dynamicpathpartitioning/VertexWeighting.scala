package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark.rdd._
import org.apache.spark.storage._
import scala.util.control.Breaks._

object VertexWeighting {
  def reduce1(kv: (Int, List[Either[Double, Integer]])) : Seq[ (Int, (Double, Double)) ] = {
    val key = kv._1
    val vals = kv._2
    var output: Seq[ (Int, (Double, Double))] = Seq() 
    val value: Double = vals(0).left.get
    vals.tail.foreach{ w => {
      output = output :+ (w.right.get: Int, (value, value * value))
    }
    }
    output
  }
  
  def vertexWeighting(vertice: RDD[(Int, Boolean)], edges: RDD[(Int, Int)], alpha: Double, iterationNumber: Int): RDD[(Int, (Double, Double))] = {
    var weight1: RDD[(Int, Double)] = vertice.mapValues { _ => 1.0 }
    var weight2: RDD[(Int, Double)] = vertice.mapValues { _ => 1.0 }

    val redges: RDD[(Int, Int)] = edges.map(t => (t._2, t._1))
    .partitionBy(edges.partitioner.get).setName("reversed edges")
    .persist(StorageLevel.MEMORY_AND_DISK)
    
    for (k <- 1 to iterationNumber) {
      /*val out1: RDD[(Int, Double)]  =
      weight1.mapValues { value => Left(value): Either[Double, Integer] }
      .union(edges.map{case (from, to) => (from, Right(to))})
      .groupByKey().mapValues(_.toList).flatMap(reduce1).reduceByKey((a, b) => (a._1+b._1, a._2+b._2))
      .mapValues(value => 1-alpha+alpha*value._1/Math.sqrt(value._2))
      .partitionBy(weight1.partitioner.get)
      
      weight1 = weight1.leftOuterJoin(out1).mapValues{case (oldWeight, newWeight) => {
        if (newWeight.isEmpty) oldWeight
        else newWeight.get
        }
      }.partitionBy(weight1.partitioner.get)
      */
      weight1 = weight1.leftOuterJoin(edges.join(weight1).map(t => (t._2._1, (t._2._2, t._2._2 * t._2._2)))
      .reduceByKey((a, b) => (a._1 + b._2, a._2 + b._2)).mapValues(t => (1-alpha)+alpha*t._1/Math.sqrt(t._2)))
      .mapValues(t => if (t._2.isEmpty) (t._1) else (t._2.get))
      
      weight2 = weight2.leftOuterJoin(redges.join(weight2).map(t => (t._2._1, (t._2._2, t._2._2 * t._2._2)))
      .reduceByKey((a, b) => (a._1 + b._2, a._2 + b._2)).mapValues(t => (1-alpha)+alpha*t._1/Math.sqrt(t._2)))
      .mapValues(t => if (t._2.isEmpty) (t._1) else (t._2.get))

    }
    /*for (k <- 1 to iterationNumber) {
      val out2: RDD[(Int, Double)]  =
      weight2.mapValues { value => Left(value): Either[Double, Integer] }
      .union(edges.map{case (to, from) => (from, Right(to))})
      .groupByKey().mapValues(_.toList).flatMap(reduce1).reduceByKey((a, b) => (a._1+b._1, a._2+b._2))
      .mapValues(value => 1-alpha+alpha*value._1/Math.sqrt(value._2))
      .partitionBy(weight2.partitioner.get)
      
      weight2 = weight2.leftOuterJoin(out2).mapValues{case (oldWeight, newWeight) => {
        if (newWeight.isEmpty) oldWeight
        else newWeight.get
        }
      }.partitionBy(weight2.partitioner.get)
      
    }*/
    val ret = weight1.join(weight2)
    ret
  }
}