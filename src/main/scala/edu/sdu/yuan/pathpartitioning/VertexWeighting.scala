package edu.sdu.yuan.pathpartitioning

import org.apache.spark.rdd._
import scala.util.control.Breaks._

object VertexWeighting {
  def reduce1(kv: (Int, List[Either[Double, Integer]])) : Seq[ (Int, Double) ] = {
    val key = kv._1
    val vals = kv._2
    var output: Seq[ (Int, Double)] = Seq() 
    val value: Double = vals(0).left.get
    vals.tail.foreach{ w => {
      output = output :+ (w.right.get: Int, value)
    }
    }
    output
  }
  
  def vertexWeighting(vertices: RDD[(Int, Int)], pairs: RDD[(Int, Int)], alpha: Double, iterationNumber: Int): RDD[(Int, (Double, Double))] = {
    var weight1: RDD[(Int, Double)] = vertices.mapValues { _ => 1.0 }
    var weight2: RDD[(Int, Double)] = vertices.mapValues { _ => 1.0 }
    
    for (k <- 1 to iterationNumber) {
      val out1: RDD[(Int, Double)]  =
      weight1.mapValues { value => Left(value): Either[Double, Integer] }
      .union(pairs.map{case (from, to) => (from, Right(to))})
      .groupByKey().mapValues(_.toList).flatMap(reduce1).reduceByKey(_+_).mapValues(value => 1-alpha+alpha*value)
    
      weight1 = weight1.leftOuterJoin(out1).mapValues{case (oldWeight, newWeight) => {
        if (newWeight.isEmpty) oldWeight
        else newWeight.get
        }
      }
    }
    
    for (k <- 1 to iterationNumber) {
      val out2: RDD[(Int, Double)]  =
      weight2.mapValues { value => Left(value): Either[Double, Integer] }
      .union(pairs.map{case (to, from) => (from, Right(to))})
      .groupByKey().mapValues(_.toList).flatMap(reduce1).reduceByKey(_+_).mapValues(value => 1-alpha+alpha*value)
    
      weight2 = weight2.leftOuterJoin(out2).mapValues{case (oldWeight, newWeight) => {
        if (newWeight.isEmpty) oldWeight
        else newWeight.get
        }
      }
    }
    
    weight1.join(weight2)
  }
}