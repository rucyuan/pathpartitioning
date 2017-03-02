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
    .union(dedges.filter{ case (flag, (_, _)) => flag}.map{ case (flag, (s, o)) => (s, o)}).cache()

    ppp.vS = ppp.generator.maintainStartingVertex(ppp.edges, dedges).cache()

    val deltaStart = ppp.vS.flatMap(t => t._2.toSeq).distinct().cache()
    
    val deltaStartPartition = deltaStart.map { t => (t, true) }.join(ppp.nodePartition)
    .map(t => (t._1, t._2._2)).collectAsMap().toMap
    deltaStart.collect().foreach { x => ppp.merger.addStartingVertex(x, deltaStartPartition.get(x)) }
    val mergedClass = ppp.classes
    .join(ppp.nodePartition.map{ case (nid, set) => (nid, set.size <= 1)})
    .map{ case (_, (classno, merged)) => (classno, merged) }
    .reduceByKey((a, b) => a && b).cache()
    val startGroup = ppp.sc.broadcast(ppp.merger.getStartGroup())
    
    val maintainence: RDD[(Int, Set[Int])] = dedges.filter{ case (flag, (_, _)) => flag}.map{ case (_, (s, o)) => (o, true)}
      .distinct().join(ppp.vS).map{ case (nid, (_, src)) => {
        val groupset = scala.collection.mutable.Set[Int]()
        src.foreach { x => 
        if (!startGroup.value.contains(x)) { groupset.add((x)) }
        else 
        { 
          groupset.add((startGroup.value.get(x).get))
        }
        }
        (nid, groupset.toSet)
      } }
    val classSeq = maintainence.join(ppp.classes).map{ case (nid, (set, classno)) => (classno, (nid, set))}
    .join(mergedClass).collect().toSeq

    val mergeResult = ppp.sc.parallelize(classSeq.filter(p => p._2._1._2.size > 1 && p._2._2).map{
        case (classno, ((nid, set), _)) => {
          val mergeSucceed = ppp.merger.merge(set)
          if (!mergeSucceed) println(classno, set)
          (classno, mergeSucceed)
        }}).distinct().filter(t => !t._2).rightOuterJoin(mergedClass).mapValues(t => t._1.isEmpty && t._2)
    mergeResult.collect().foreach(println)
    ppp.merger.fillPart()
    
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

  }
}
