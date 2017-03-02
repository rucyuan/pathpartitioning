package edu.sdu.yuan.pathpartitioning

import org.apache.spark.rdd._
import scala.util.control.Breaks._

object GeneratingStartVertices {
  val TripleRegex = """(\d+)\s(\d+)\s(\d+)""".r 
  def toTriple(line:String) : Seq[(Int, (Int, Int))] = line match {
    case TripleRegex(t1, t2, t3) => Seq( (t1.toInt, (t2.toInt, t3.toInt)) )
    case _ => Seq()
  }
  def toPair(line:String) : Seq[(Int, Int)] = line match {
    case TripleRegex(t1, t2, t3) => Seq( (t1.toInt, t3.toInt) )
    case _ => Seq()
  }
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
  def generatingStartVertices(vertices: RDD[(Int, Int)], pairs: RDD[(Int, Int)]):RDD[(Int, Set[Int])] = {
    var out: RDD[(Int, (Boolean, Option[Int], Option[Set[Int]]))] = vertices
    .map{
      case (nid, src) => 
        if (src < 0) (nid, (true, Some(nid), Some(Set(nid)))) else (nid, (true, Some(nid), Some(Set())))
      }
    breakable {
    while (true) {
    out = out.union(pairs.map{case (from, to) => (from, (false, Some(to), None))}).groupByKey.mapValues(_.toList).flatMap(reduce1)
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
    }
    out = out.mapValues{ case (_, m, src) => {
      if (src.get.size <= 0)
        (true, m, Some(Set(m.get)))
      else
        (false, m, src)
    } 
    }
    breakable {
    while (true) {
    out = out.union(pairs.map{case (from, to) => (from, (false, Some(to), None))}).groupByKey.mapValues(_.toList).flatMap(reduce1)
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
    }
    val vS: RDD[(Int, Set[Int])] = out.mapValues{case (_, _, src)=>{
      src.get
    }
    }
    vS
  }
}