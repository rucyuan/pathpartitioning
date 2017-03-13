package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.control.Breaks._

class StartingVertexGenerator() extends Serializable {
  var output: RDD[(Int, (Set[Int], Int))] = null
  var startVertice: Set[Int] = null
  def generateStartingVertex(vertex: RDD[(Int, Boolean)], edge: RDD[(Int, Int)]): RDD[(Int, Set[Int])] = {
    var out: RDD[(Int, (Set[Int], Int, Boolean))] = vertex.map( t => 
      if (t._2) (t._1, (Set[Int](t._1), t._1, true))
      else (t._1, (Set[Int](), t._1, true))
    ).partitionBy(vertex.partitioner.get)
    
    startVertice = vertex.filter(t => t._2).map(t => t._1).collect().toSet
    
    breakable {
      while (true) {
        out = out.leftOuterJoin(edge.join(out.filter(t => t._2._3)).map(t => (t._2._1, (t._2._2._1, t._2._2._2)))
        .reduceByKey((a, b) => (a._1.union(b._1), Math.min(a._2, b._2)))
        ).mapValues(t => if (t._2.isEmpty) (t._1._1, t._1._2, false)
        else
          (t._1._1.union(t._2.get._1), Math.min(t._1._2, t._2.get._2), t._1._1.diff(t._2.get._1).size > 0 || t._1._2 < t._2.get._2) )
        
        if (out.filter(t => t._2._3).count() == 0) break
      }
    }
    output = out.mapValues{ case (src, m, _) => {
      (src, m)
    }}.partitionBy(out.partitioner.get)
    var ou = out.mapValues(t => if (t._1.isEmpty) (Set[Int](t._2), true) else (t._1, false))
    .partitionBy(out.partitioner.get)
    
    startVertice ++= out.filter(t => t._2._1.isEmpty).map(t => t._2._2).collect().toSet
    
    breakable {
      while (ou.filter(t => t._2._2).count() > 0) {
        ou = edge.join(ou.filter(t => t._2._2)).map(t => (t._2._1, t._2._2._1))
        .reduceByKey((a, b) => a.union(b))
        .join(ou).mapValues(t => (t._1.union(t._2._1), t._1.diff(t._2._1).size > 0) )
        .partitionBy(ou.partitioner.get)
      }
    }
    ou.mapValues(t => t._1)
  }
def maintainStartingVertex(edge: RDD[(Int, Int)], dedges: RDD[(Boolean, (Int, Int))]) : RDD[(Int, Set[Int])] = {
    val vertice = edge.flatMap{ case (from, to) => Seq((from, true), (to, false))}
    .reduceByKey((a, b) => a && b).cache()
    
    startVertice = vertice.filter(t => t._2).map(t => t._1).collect().toSet
    
    var out: RDD[(Int, (Set[Int], Int, Set[Int], Int))] = 
      output.map{ case (nid, (src, m)) => (nid, (src, m, Set[Int](), Int.MaxValue))}
    .partitionBy(output.partitioner.get).cache() 
    
    val deletedEdges: RDD[(Int, Int)] = dedges.filter(t => !t._1).map{ case (_, (from, to)) => (from, to)}
    .partitionBy(edge.partitioner.get).cache()
    val addedEdges: RDD[(Int, Int)] = dedges.filter(t => t._1).map{ case (_, (from, to)) => (to, from)}
    .partitionBy(edge.partitioner.get).cache()
    val orginalEdges: RDD[(Int, Int)] = edge.union(deletedEdges)
    .partitionBy(edge.partitioner.get).cache()
    var deletedSet: RDD[(Int, (Set[Int], Int))] = 
      out.join(addedEdges)
      .filter(f => f._2._1._1.size == 1 && f._2._1._1.last == f._1)
      .map(f => (f._1, (f._2._1._1, Int.MaxValue))).union(
      out.join(deletedEdges)
      .map{ case (nid, ((src, m, _, _), to)) => 
         (to, (src, m))
        }
      .reduceByKey((a, b) => (a._1.union(b._1), Math.min(a._2, b._2))))
      .partitionBy(out.partitioner.get)
    
    breakable {
        while (true) {
      out = out.leftOuterJoin(deletedSet)
      .map{ case (nid, ((src, m, _, _), del)) => {
          if (del.isDefined) {
            if (m == del.get._2)
            (nid, (src.diff(del.get._1), nid, src.intersect(del.get._1), m))
            else
            (nid, (src.diff(del.get._1), m, src.intersect(del.get._1), Int.MaxValue))
          } else {
            (nid, (src, m, Set[Int](), Int.MaxValue))
          }
          }}.partitionBy(out.partitioner.get)
      deletedSet = edge.union(deletedEdges).join(out)
      .map{ case (nid, (to, (_, _, delsrc, delm))) => (to, (delsrc, delm)) }
        .reduceByKey( (a, b) => (a._1.union(b._1), Math.min(a._2, b._2)) )
        .filter{ case (_, (src, m)) => !src.isEmpty || m < Int.MaxValue}
        .partitionBy(out.partitioner.get)
      if (deletedSet.count() <= 0) break
        }
    }
    
    var out1: RDD[(Int, (Set[Int], Int, Boolean))] = out
    .map{ case (nid, (src, m, _, _)) => (nid, (src, m))}
    .rightOuterJoin(vertice).map{ case (nid, (src, start)) => {
      if (start == true) {
        if (src.isEmpty || src.get._1.isEmpty) (nid, (Set[Int](nid), nid, true))
        else (nid, (src.get._1, src.get._2, true))
      } else {
        if (src.isEmpty || src.get._1.isEmpty) (nid, (Set[Int](), nid, true))
        else (nid, (src.get._1, src.get._2, true))
      }
    } }.partitionBy(out.partitioner.get).cache()
    
    var changedSet: RDD[(Int, (Set[Int], Int))] = edge.join(out1)
    .flatMap{case (from, (to, (src, m, updated))) => {
      if (updated) Seq((to, (src, m)))
      else Seq()
    }}.reduceByKey((a, b) => (a._1.union(b._1), Math.min(a._2, b._2)))
    .partitionBy(out1.partitioner.get).cache()

    breakable {
      while (true) {
      out1 = out1.leftOuterJoin(changedSet).map{ case (nid, ((src, m, updated), changed)) => {
        if (changed.isEmpty) {
          (nid, (src, m, false))
        } else {
        (nid, (src.union(changed.get._1), Math.min(m, changed.get._2), src.intersect(changed.get._1).size < changed.get._1.size || changed.get._2 < m))
        }
      }}.partitionBy(out1.partitioner.get)

      changedSet = edge.join(out1)
    .flatMap{case (from, (to, (src, m, updated))) => {
      if (updated) Seq((to, (src, m)))
      else Seq()
    }}.reduceByKey((a, b) => (a._1.union(b._1), Math.min(a._2, b._2)))
    .partitionBy(out1.partitioner.get)
    
      if (changedSet.count() <= 0) break
      }
    }

    output = out1.map{ case (nid, (src, m, _)) => (nid, (src, m))}
    .partitionBy(out1.partitioner.get).cache()
    
    var fout :RDD[(Int, (Boolean, Set[Int]))] = out1.map{ case (nid, (src, m, _)) => {
      if (src.size == 0 && nid == m) (nid, (true, Set[Int](m)))
      else (nid, (false, src))
    }}.partitionBy(out1.partitioner.get)
    
    startVertice ++= out1.filter(t => t._2._1.isEmpty).map(t => t._2._2).collect().toSet

    /*breakable {
      while (true) {
        fout = edge.join(fout)
        
        
      }
    }*/
     val vS: RDD[(Int, Set[Int])] = fout.map{ case (nid, (_, src)) => (nid, src)}
    .partitionBy(fout.partitioner.get)
     vS
  }
}