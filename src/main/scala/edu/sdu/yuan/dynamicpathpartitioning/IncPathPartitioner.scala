package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.control.Breaks._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*class IncPathPartitioner(pp: InitPathPartitioner, input: RDD[(Boolean, (Int, Int, Int))]){
  val sc: SparkContext = pp.sc
  val merger: PathGroupMerger = pp.merger
  val dtriples: RDD[(Boolean, (Int, Int, Int))] = input  
  val dedges: RDD[(Boolean, (Int, Int))] = dtriples.map(triple => (triple._1, (triple._2._1, triple._2._3)))
  val edges: RDD[(Int, Int)] = pp.edges
  .subtract(dedges.filter{ case (flag, (_, _)) => !flag}.map{ case (flag, (s, o)) => (s, o)})
  .union(dedges.filter{ case (flag, (_, _)) => flag}.map{ case (flag, (s, o)) => (s, o)}).cache()
  val generator: StartingVertexGenerator = pp.generator
  val vS: RDD[(Int, Set[Int])] = generator.maintainStartingVertex(edges, dedges).cache()
  val classes: RDD[(Int, Int)] = pp.classes
  val mergedClass = classes.join(pp.merged)
  .map{ case (_, (classno, merged)) => (classno, merged) }
  .reduceByKey((a, b) => a && b).cache()
  
  val maintainence: RDD[(Int, Set[Int])] = IncPathPartitioner.getMaintainence(this)
  
  val classSeq = maintainence.join(classes).map{ case (nid, (set, classno)) => (classno, (nid, set))}
  .join(mergedClass).collect().toSeq
  val mergeResult = sc.parallelize(classSeq.filter(p => p._2._1._2.size > 1 && p._2._2).map{
        case (classno, ((nid, set), _)) => {
          (classno, merger.merge(set))
        }}).filter(t => !t._2).rightOuterJoin(mergedClass).mapValues(t => t._1.isEmpty && t._2)
   //mergeResult.saveAsObjectFile("mergeresult")
   merger.fillPart()
   
   val result: RDD[(Int, Set[Int])] = 
      sc.parallelize(classSeq.flatMap{ case (_, ((nid, set), _)) => {
         set.map(x => {
           (nid, merger.getPart(x))
         }).toSeq
       }
    }).map(t => (t._1, Set[Int](t._2))).reduceByKey((a, b) => a.union(b))
   
   var out = result.leftOuterJoin(pp.nodePartition).map{
      case (nid, (set1, set2)) => {
        if (set2.isEmpty) (nid, set1)
        else (nid, set1.diff(set2.get))
      } }.filter( t => t._2.size > 0).map( x => (x._1, (x._2, true)))

   var changedSet: RDD[(Int, Set[Int])] = null
   breakable {
       while (true) {
          changedSet = edges.join(out).flatMap{ case (from, (to, (set, updated))) => {
            if (updated) {
              Seq((to, set))
            } else Seq()
          }}.reduceByKey((a, b) => a.union(b))
          
          out = out.map(p => (p._1, p._2._1)).fullOuterJoin(changedSet).map{ case (nid, (old, added)) => {
            if (added.isEmpty)
              (nid, (old.get, false))
            else if (old.isEmpty)
              (nid, (added.get, true))
            else
            (nid, (old.get.union(added.get), old.get.intersect(added.get).size < added.get.size))
          }}
          
          if (out.filter(p => p._2._2).count() <= 0) break
       }
   }

   out = out.leftOuterJoin(pp.nodePartition).map{
      case (nid, ((set1, _), set2)) => {
        if (set2.isEmpty) (nid, set1)
        else (nid, set1.diff(set2.get))
      } }.filter( t => t._2.size > 0).map( x => (x._1, (x._2, false)))
   
   val addedTriples = pp.initialTriples.join(out)
   .flatMap(t => t._2._2._1.map { x => (x, (t._1, t._2._1._1, t._2._1._2)) }
   .toSeq).union(IncPathPartitioner.getAddedTriples(this, pp.nodePartition))
   .partitionBy(new HashPartitioner(pp.partitionNum))
   val deletedTriples = sc.parallelize(0 until pp.partitionNum)
   .cartesian(dtriples.filter(t => !t._1).map(t => t._2))
   .partitionBy(new HashPartitioner(pp.partitionNum))
   
   val triples: RDD[(Int, (Int, Int, Int))] = pp.triples.union(addedTriples)
   .subtract(deletedTriples).cache()
}*/

object IncPathPartitioner extends Serializable{
  def maintainPPP(ppp: PathPartitioningPlan, feeder: DataFeeder): Unit = {
    val dtriples: RDD[(Boolean, (Int, Int, Int))] = feeder.getNextDeltaInput().cache()
    val dedges: RDD[(Boolean, (Int, Int))] = dtriples.map(triple => (triple._1, (triple._2._1, triple._2._3))).cache()
    
    ppp.edges = ppp.edges
    .subtract(dedges.filter{ case (flag, (_, _)) => !flag}.map{ case (flag, (s, o)) => (s, o)})
    .union(dedges.filter{ case (flag, (_, _)) => flag}.map{ case (flag, (s, o)) => (s, o)})
    .partitionBy(ppp.edges.partitioner.get).cache()
    
    ppp.vertices = ppp.edges.flatMap{
      case (from, to) => Seq((to, false), (from, true))
    }.reduceByKey{case (a, b) => a && b}
  .partitionBy(ppp.edges.partitioner.get).cache()

    ppp.vS = ppp.generator.maintainStartingVertex(ppp.edges, dedges).cache()
    
    ppp.merger.extendTo(ppp.generator.startVertice.max+1)
    
    val np = ppp.nodePartition.collect().toMap
    ppp.generator.startVertice.foreach(st => ppp.merger.addStartingVertex(st, np.get(st)))
    
    ppp.merger.refreshStartingNum()
    
    val startGroup = ppp.sc.broadcast(ppp.merger.getStartGroup())
    
    val maintainence: RDD[(Int, Set[Int])] = dedges.filter(t => t._1)
    .map(t => (t._2._2, true)).distinct().join(ppp.vS)
    .map{ case (nid, (_, src)) => {
        var groupset: Set[Int] = Set[Int]()
        src.foreach { x => 
          if (!startGroup.value.contains(x)) groupset += x
          else groupset += startGroup.value.get(x).get
        }
        (nid, groupset)
      } }
    val classesList =  ppp.sc.parallelize(ppp.mergedClasses.toSeq).join(ppp.classes.map(t => (t._2, t._1)))
    .map(t => (t._2._2, (t._1, t._2._1))).join(ppp.vS).map(t => (t._2._1, (t._1, t._2._2)))
    .groupByKey().collect().toList.sortBy(t => t._1._2)
    
    val verticeList = ppp.sc.parallelize(ppp.mergedVertice.toSeq).join(ppp.vS).collect()
    .toList.sortBy(t => t._2._1)
    
    println("Old:", ppp.mergedClasses.size, ppp.mergedVertice.size)    
    
    classesList.foreach( t => {
        val result = ppp.merger.merge(t._2.toSeq, true)
        if (!result) { 
          ppp.mergedClassesNum -= 1
          ppp.mergedClasses -= (t._1)
          ppp.mergedVerticeNum -= t._2.size
        }
    })
    
    verticeList.foreach( t => {
        val result = ppp.merger.merge(Seq((t._1, t._2._2)), true)
        if (!result) { 
          ppp.mergedVerticeNum -= 1
          ppp.mergedVertice -= ((t._1, t._2._1))
        }
    })
    
    println("New:", ppp.mergedClasses.size, ppp.mergedVertice.size)
    
    ppp.merger.fillPartition()
    
    val nodePartition = ppp.sc.broadcast(ppp.merger.nodePartition)
    
    val newNodePartition = ppp.vS.mapValues(set => {
      set.map { st => nodePartition.value.get(st).get }
    }).partitionBy(ppp.vS.partitioner.get).cache()
    
    val delta1 = newNodePartition.fullOuterJoin(ppp.nodePartition)
    .mapValues(t => t._1.getOrElse(Set[Int]()).diff(t._2.getOrElse(Set[Int]())))
    .filter(t => t._2.size > 0).join(ppp.triples)
    .flatMap(t => t._2._1.map { p => (p, (t._1, t._2._2._1, t._2._2._2)) })
    .partitionBy(ppp.result.partitioner.get)
    //delta1.collect().foreach(println)
    //println("====================================")
    val delta2 = newNodePartition.fullOuterJoin(ppp.nodePartition)
    .mapValues(t => t._2.getOrElse(Set[Int]()).diff(t._1.getOrElse(Set[Int]())))
    .filter(t => t._2.size > 0).join(ppp.triples)
    .flatMap(t => t._2._1.map { p => (p, (t._1, t._2._2._1, t._2._2._2)) })
    .partitionBy(ppp.result.partitioner.get)
    //delta2.collect().foreach(println)
    //println("====================================")
    val delta3 = newNodePartition.join(dtriples.filter(t => t._1)
        .map(t => (t._2._1, (t._2._2, t._2._3))))
    .flatMap(t => t._2._1.map { p => (p, (t._1, t._2._2._1, t._2._2._2)) })
    .partitionBy(ppp.result.partitioner.get)
    //delta3.collect().foreach(println)
    //println("====================================")
    val delta4 = ppp.nodePartition.join(dtriples.filter(t => !t._1)
        .map(t => (t._2._1, (t._2._2, t._2._3))))
    .flatMap(t => t._2._1.map { p => (p, (t._1, t._2._2._1, t._2._2._2)) })
    .partitionBy(ppp.result.partitioner.get)
    delta4.collect().foreach(println)
    
    ppp.dataMovement = delta1.count()+delta2.count()+delta3.count()+delta4.count()
    
    ppp.result = ppp.result.union(delta1).subtract(delta2).union(delta3)
    .subtract(delta4).partitionBy(ppp.result.partitioner.get).cache()
    
    ppp.triples = ppp.triples.union(dtriples.filter(t => t._1).map(t => (t._2._1, (t._2._2, t._2._3))))
    .subtract(dtriples.filter(t => !t._1).map(t => (t._2._1, (t._2._2, t._2._3)))).cache()
   
    ppp.nodePartition = newNodePartition
    
    
    /*
    val nodePartition = ppp.sc.broadcast(ppp.merger.nodePartition)
    
    val result: RDD[(Int, Set[(Int, Int)])] = 
    dedges.filter(t => t._1).map(t => (t._2._2, true))
    .distinct().join(ppp.vS).map( t => 
      (t._1, 
          t._2._2.seq.map( x => nodePartition.value.get(x).get).groupBy(t => t)
          .map(t => (t._1, t._2.size)).toSet
      )
    ).union(ppp.sc.parallelize(ppp.generator.startVertice
        .map(st => (st, Set((ppp.merger.nodePartition(st), 1)))).toSeq)).cache()
  
    ppp.nodePartition.join(result).map(t => (t._1, 
        {
          val map = t._2._2.toMap
          t._2._1.map(t => (t._1, -t._2+map.getOrElse(t._1, 0)))
          .filter(t => t._2 != 0)
        })).filter(t => t._2.size > 0)
        .collect().foreach(println)
    
    ppp.nodePartition.rightOuterJoin(result).map(t => (t._1,
        {
          val map = t._2._1.getOrElse(Set[(Int, Int)]()).toMap
          t._2._2.map(t => (t._1, t._2-map.getOrElse(t._1, 0)))
          .filter(t => t._2 != 0)
        })).filter(t => t._2.size > 0).collect().foreach(println)
    
    //result.collect().foreach(println)
*/
    /*
    val result: RDD[(Int, Set[Int])] = 
      ppp.sc.parallelize(classSeq.flatMap{ case (_, ((nid, set), _)) => {
         set.map(x => {
           (nid, ppp.merger.getPart(x))
         }).toSeq
       }
    } ++ ppp.merger.getStartPart().toSeq
      ).map(t => (t._1, Set[Int](t._2))).reduceByKey((a, b) => a.union(b))

    var out = result.leftOuterJoin(ppp.nodePartition).map{
      case (nid, (set1, set2)) => {
        if (set2.isEmpty) (nid, set1)
        else (nid, set1.diff(set2.get))
      } }.filter( t => t._2.size > 0).map( x => (x._1, (x._2, true)))

   var changedSet: RDD[(Int, Set[Int])] = null
   breakable {
       while (true) {
          changedSet = ppp.edges.join(out).flatMap{ case (from, (to, (set, updated))) => {
            if (updated) {
              Seq((to, set))
            } else Seq()
          }}.reduceByKey((a, b) => a.union(b))
          
          out = out.map(p => (p._1, p._2._1)).fullOuterJoin(changedSet).map{ case (nid, (old, added)) => {
            if (added.isEmpty)
              (nid, (old.get, false))
            else if (old.isEmpty)
              (nid, (added.get, true))
            else
            (nid, (old.get.union(added.get), old.get.intersect(added.get).size < added.get.size))
          }}
          if (out.filter(p => p._2._2).count() <= 0) break
       }
   }

   out = out.leftOuterJoin(ppp.nodePartition).map{
      case (nid, ((set1, _), set2)) => {
        if (set2.isEmpty) (nid, set1)
        else (nid, set1.diff(set2.get))
      } }.filter( t => t._2.size > 0).map( x => (x._1, (x._2, false)))
   
   val startPart = ppp.sc.broadcast(ppp.merger.getStartPart())

   val addedTriples: RDD[(Int, (Int, Int, Int))] = ppp.triples.join(out)
   .flatMap(t => t._2._2._1.map { x => (x, (t._1, t._2._1._1, t._2._1._2)) }
   .toSeq).union(
    dtriples.filter(t => t._1).map(t => (t._2._1, true)).distinct().join(ppp.vS)
    .mapValues( t => {
      t._2.map(x => startPart.value.get(x).get)
    }).flatMapValues(t => t.toSeq)
    .join(dtriples.filter(t => t._1).map(t => (t._2._1, (t._2._2, t._2._3))))
    .map(t => (t._2._1, (t._1, t._2._2._1, t._2._2._2)))
   )
   .partitionBy(ppp.result.partitioner.get)
   
   val deletedTriples: RDD[(Int, (Int, Int, Int))] = ppp.sc.parallelize(0 until ppp.partitionNum)
   .cartesian(dtriples.filter(t => !t._1).map(t => t._2))
   .partitionBy(ppp.result.partitioner.get)
   
   ppp.result = ppp.result.union(addedTriples)
   .subtract(deletedTriples).partitionBy(ppp.result.partitioner.get).cache()
   
   ppp.triples = ppp.triples.union(dtriples.filter(t => t._1).map(t => (t._2._1, (t._2._2, t._2._3))))
   .subtract(dtriples.filter(t => !t._1).map(t => (t._2._1, (t._2._2, t._2._3)))).cache()

   ppp.nodePartition = ppp.vS.mapValues(set => set.map(x => startPart.value.get(x).get)).cache()
*/
  }
}
