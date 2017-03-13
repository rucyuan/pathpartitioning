package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

class DataFeeder(sc: SparkContext, insertionRatio: Double, deletionRatio: Double, num: Int, inputname: String, dictionaryname: String) {
  val lines: RDD[String] = sc.textFile(inputname)
  val triples: RDD[(Int, (Int, Int))] = lines.map(line => (line.split(" ")(0).toInt, (line.split(" ")(1).toInt, line.split(" ")(2).toInt)))
  var updatetimes: Int = -1
  var addition: RDD[(Int, (Int, Int))] = triples.sample(false, insertionRatio * num, 0)
  val initial: RDD[(Int, (Int, Int))] = triples.subtract(addition)
  var deletion: RDD[(Int, (Int, Int))] = initial.sample(false, (deletionRatio * num)/(1-insertionRatio * num), 0)
  
  var delta: Array[RDD[(Boolean, (Int, Int, Int))]] = Array[RDD[(Boolean, (Int, Int, Int))]]()
  
  for (k <- Range(0, num)) {
    val a = addition.sample(false, 1.0/(num - k), 0)
    val d = deletion.sample(false, 1.0/(num - k), 0)
    addition = addition.subtract(a)
    deletion = deletion.subtract(d)
    delta ++= Array( a.map(t => (true, (t._1, t._2._1, t._2._2)))
        .union(d.map(t => (false, (t._1, t._2._1, t._2._2)))) )
  }

  val entities: RDD[(Int, String, Int)] =  sc.textFile(dictionaryname).cache()
    .map ( line => (line.split(" ")(0).toInt, line.split(" ")(1),
        line.split(" ")(2).toInt )).cache()
        
  def getInitialInput(batchNumber: Int): RDD[(Int, (Int, Int))] = {
    if (batchNumber < 0) initial
    else {
      var out = initial
      for (x <- Range(0, batchNumber+1)) {
        out = out.subtract(delta(x).filter(t => !t._1).map(t => (t._2._1, (t._2._2, t._2._3))))
        .union(delta(x).filter(t => t._1).map(t => (t._2._1, (t._2._2, t._2._3))))
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
    entities.map{ case (id, uri, attr) => { 
      if (attr == 0) {
        (id, "<" + uri + ">")
        } else {
          (id, "\"" + uri + "\"")
          }
      }
    }.collectAsMap().toMap
  }
  def getClasses(): RDD[(Int, Int)] = {
    val literal = entities.filter(t => t._3 == 1).map( t => (t._1, t._1))
    triples.filter(t => t._2._1 == 0).map(t => (t._1, t._2._2)).union(literal)
    .rightOuterJoin(entities.keyBy(t => t._1)).flatMap(t => {
      if (t._2._1.isEmpty) Seq()
      else Seq((t._1, t._2._1.get))
    })
  }
}