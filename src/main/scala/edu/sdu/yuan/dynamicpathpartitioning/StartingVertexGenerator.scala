package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.control.Breaks._

class StartingVertexGenerator(vertex: RDD[(Int, Boolean)], edge: RDD[(Int, Int)]) extends Serializable {
  var output: RDD[(Int, (Set[Int], Int))] = null
  //var out: RDD[(Int, (Boolean, Option[Int], Option[Set[Int]]))] = null
  var out: RDD[(Int, (Set[Int], Int, Boolean))] = null
  def reduce1(kv: (Int, List[(Boolean, Option[Int], Option[Set[Int]])])) : Seq[ (Int, (Boolean, Option[Int], Option[Set[Int]])) ] = {
    val key = kv._1
    val vals = kv._2
    val active = vals(0)._1
    val m = vals(0)._2
    val src = vals(0)._3
    var output: Seq[ (Int, (Boolean, Option[Int], Option[Set[Int]]) ) ] = Seq( (key, (false, m, src)) )
    
    if (active) 
    vals.tail.foreach{ case (_, w, _) => {
      output = output :+ ( (w.get, (true, Some(Math.min(w.get, m.get)), src)) )
    }
    
    }
    output
  }
  def generateStartingVertex(): Unit = {
    out = vertex.map( t => 
      if (t._2) (t._1, (Set[Int](t._1), t._1, true))
      else (t._1, (Set[Int](), t._1, true))
    ).partitionBy(vertex.partitioner.get)

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

    /*out = vertex.map{
      case (nid, src) => 
        if (src) (nid, (true, Some(nid), Some(Set(nid).toSet))) else (nid, (true, Some(nid), Some(Set().toSet)))
      }
    
    breakable {
    while (true) {
      val t0 = System.nanoTime()
    out = out.union(edge.map{case (from, to) => (from, (false, Some(to), None))}).groupByKey.mapValues(_.toList).flatMap(reduce1)
    .reduceByKey{case (a, b) => {
      val m = Math.min(a._2.get, b._2.get)
      val src = a._3.get ++ b._3.get
      var changed:Boolean = true
      if (a._1 == false && a._2.get == m && a._3.get == src) changed = false   
      if (b._1 == false && b._2.get == m && b._3.get == src) changed = false
      (changed, Some(m), Some(src))
    }
    }
      val t1 = System.nanoTime()
     if (out.filter{case (_, (active, _, _)) => active}.count() <= 0) break
     val t2 = System.nanoTime()
     println((t1-t0)/1e6.toLong, (t2-t1)/1e6.toLong)
    }
    }
    
    
    output = out.mapValues{ case (_, m, src) => {
      (src.get, m.get)
    }
    }.cache()*/
    
  }

  def maintainStartingVertex(edge: RDD[(Int, Int)], dedges: RDD[(Boolean, (Int, Int))]) : RDD[(Int, Set[Int])] = {
    val start = edge.flatMap{ case (from, to) => Seq((from, true), (to, false))}
    .reduceByKey((a, b) => a && b).filter(f => f._2).map(f => f._1).collect().toSet
    
    val startVertices = edge.flatMap{ case (from, to) => Seq((from, true), (to, false))}
    .reduceByKey((a, b) => a && b).cache()
    
    var out: RDD[(Int, (Set[Int], Int, Set[Int], Int))] = output.map{ case (nid, (src, m)) => (nid, (src, m, Set[Int](), Int.MaxValue))}.cache() 

    val deletedEdges: RDD[(Int, Int)] = dedges.filter{ case (add, (from, to)) => !add}.map{ case (_, (from, to)) => (from, to)}.cache()
    val addedEdges: RDD[(Int, Int)] = dedges.filter{ case (add, (from, to)) => add}.map{ case (_, (from, to)) => (to, from)}.cache()
    
    var deletedSet: RDD[(Int, (Set[Int], Int))] = 
      out.join(addedEdges)
      .filter(f => f._2._1._1.size == 1 && f._2._1._1.last == f._1)
      .map(f => (f._1, (f._2._1._1, Int.MaxValue))).union(
      out.join(deletedEdges)
      .map{ case (nid, ((src, m, _, _), to)) => 
         (to, (src, m))
        }
      .reduceByKey((a, b) => (a._1.union(b._1), Math.min(a._2, b._2)))).cache()
    
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
          }}.cache()
      deletedSet = edge.union(deletedEdges).join(out)
      .map{ case (nid, (to, (_, _, delsrc, delm))) => (to, (delsrc, delm)) }
        .reduceByKey( (a, b) => (a._1.union(b._1), Math.min(a._2, b._2)) )
        .filter{ case (_, (src, m)) => !src.isEmpty || m < Int.MaxValue}.cache()
      if (deletedSet.count() <= 0) break
        }
    }

    var out1: RDD[(Int, (Set[Int], Int, Boolean))] = out
    .map{ case (nid, (src, m, _, _)) => (nid, (src, m))}
    .rightOuterJoin(startVertices).map{ case (nid, (src, start)) => {
      if (start == true) {
        if (src.isEmpty || src.get._1.isEmpty) (nid, (Set[Int](nid), nid, true))
        else (nid, (src.get._1, src.get._2, true))
      } else {
        if (src.isEmpty || src.get._1.isEmpty) (nid, (Set[Int](), nid, true))
        else (nid, (src.get._1, src.get._2, true))
      }
    } }.cache()
    
    
    
    var changedSet: RDD[(Int, (Set[Int], Int))] = edge.join(out1)
    .flatMap{case (from, (to, (src, m, updated))) => {
      if (updated) Seq((to, (src, m)))
      else Seq()
    }}.reduceByKey((a, b) => (a._1.union(b._1), Math.min(a._2, b._2))).cache()

    breakable {
      while (true) {
      out1 = out1.leftOuterJoin(changedSet).map{ case (nid, ((src, m, updated), changed)) => {
        if (changed.isEmpty) {
          (nid, (src, m, false))
        } else {
        (nid, (src.union(changed.get._1), Math.min(m, changed.get._2), src.intersect(changed.get._1).size < changed.get._1.size || changed.get._2 < m))
        }
      }}.cache()

      changedSet = edge.join(out1)
    .flatMap{case (from, (to, (src, m, updated))) => {
      if (updated) Seq((to, (src, m)))
      else Seq()
    }}.reduceByKey((a, b) => (a._1.union(b._1), Math.min(a._2, b._2))).cache()
    
      if (changedSet.count() <= 0) break
      }
    }

    output = out1.map{ case (nid, (src, m, _)) => (nid, (src, m))}.cache()
    var fout :RDD[(Int, (Boolean, Set[Int]))] = out1.map{ case (nid, (src, m, _)) => {
      if (src.size == 0 && nid == m) (nid, (true, Set[Int](m)))
      else (nid, (false, src))
    }}.cache()
    /*breakable {
      while (true) {
        fout = edge.join(fout)
        
        
      }
    }*/
     val vS: RDD[(Int, Set[Int])] = fout.map{ case (nid, (_, src)) => (nid, src)}
     vS
  }
  
  def getStartingVertex() : RDD[(Int, Set[Int])] = {
    var ou = out.mapValues(t => if (t._1.isEmpty) (Set[Int](t._2), true) else (t._1, false))
    .partitionBy(out.partitioner.get)

    breakable {
      while (ou.filter(t => t._2._2).count() > 0) {
        ou = edge.join(ou.filter(t => t._2._2)).map(t => (t._2._1, t._2._2._1))
        .reduceByKey((a, b) => a.union(b))
        .join(ou).mapValues(t => (t._1.union(t._2._1), t._1.diff(t._2._1).size > 0) )
        .partitionBy(ou.partitioner.get)
      }
    }
    /*breakable {
    while (true) {
    out = out.union(edge.map{case (from, to) => (from, (false, Some(to), None))}).groupByKey.mapValues(_.toList).flatMap(reduce1)
    .reduceByKey{case (a, b) => {
      val m = Math.min(a._2.get, b._2.get)
      val src = a._3.get ++ b._3.get
      var changed:Boolean = true
      if (a._1 == false && a._2.get == m && a._3.get == src) changed = false   
      if (b._1 == false && b._2.get == m && b._3.get == src) changed = false
      (changed, Some(m), Some(src))
    }
    }
     if (out.filter{case (_, (active, _, _)) => active}.count() <= 0) break
    }
    }*/
    ou.mapValues(t => t._1)
  }
  
}