package edu.sdu.yuan.pathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._


object PathPartitioning {
  val usage: String = """
        Usage: pathpartitioning [--partitionNum num] [--alpha num] [--iterationNum num] --input inputname --output outputname
      """
  var partitionNum: Int = 10
  var alpha: Double = 0.8
  var iterNum: Int = 5 
  var inputname: String = ""
  var outputname: String = ""
  def main(args: Array[String]) = {
    if (args.length == 0) println(usage)
    //args.sliding(2, 2).toList.foreach(x => println(x(0)+" "+x(1)))
    args.sliding(2).toList.foreach {
      case Array("--partitionNum", k: String) => partitionNum = k.toInt
      case Array("--alpha", a: String) => alpha = a.toDouble
      case Array("--iterationNum", iter: String) => iterNum = iter.toInt
      case Array("--input", input: String) => inputname = input
      case Array("--output", output: String) => outputname = output
      case _ => None
      }
    
    val conf = new SparkConf().setAppName("PathPartitioning").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val graph = sc.textFile(inputname)
    val pairs: RDD[(Int, Int)] = graph
    .flatMap(line => GeneratingStartVertices.toPair(line))
    
    val vertices: RDD[(Int, Int)] = pairs.flatMap{
      case (from, to) => Seq((to, from), (from, -1))
    }
    .reduceByKey{case (a, b) => Math.max(a, b)}

    val vS: RDD[(Int, Set[Int])] = GeneratingStartVertices.generatingStartVertices(vertices, pairs)
    
    val weights: RDD[(Int, Double)] = VertexWeighting.vertexWeighting(vertices, pairs, alpha, iterNum).map{case (id, (w1, w2)) => (id, w1 * w2)}
    
    val sortedList: Array[(Int, (Set[Int], Double))] =  vS.join(weights).collect().sortWith((left, right) => left._2._2 > right._2._2)
    
    val nodePartition: RDD[(Int, Set[Int])] = sc.parallelize(BottomUpMerging.bottomUpMerging(sortedList, partitionNum))
    
    val triples: RDD[(Int, (Int, Int, Int))] = graph
    .flatMap(line => GeneratingStartVertices.toTriple(line)).join(nodePartition)
    .map{ case (s, ((p, o), set)) => {
      (o, ((s, set), p))
    }
    }.join(nodePartition).map{ case (o, (((s, setS), p), setO)) => {
      ((s, setS), (o, setO), p)
    }
    }
    .flatMap{ case ((s, setS), (o, setO), p) => {
      setS.intersect(setO).toSeq.map { ele => (ele, (s, p, o)) }
    }
    }
    .partitionBy(new HashPartitioner(partitionNum))
    
    /*triples.mapPartitionsWithIndex( (index: Int, it: Iterator[(Int, (Int, Int, Int))]) => {
      it.toList.map{case (pid, t) => (index, (pid, t))}.iterator
    }).collect().foreach(println)*/
    
    triples.saveAsTextFile(outputname)
    
  }
  
}