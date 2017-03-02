package edu.sdu.yuan.dynamicpathpartitioning

import scala.util.control.Breaks._
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ListBuffer

class PathGroupMerger(partNum: Int, classBased: Boolean = false) extends Serializable {
  var startNum: Int = 0
  var maxSize: Int = 0
  var nodeToStart = Map[Int, Int]()
  var startToPart = Map[Int, Int]()
  val sortedPartitions: PriorityQueue[(Int, Int)] = new PriorityQueue[(Int, Int)]()(Ordering.by((t) => -t._2))
  val startToNode = ListBuffer[Int]()
  val ds: DisjointSet = new DisjointSet(startNum)
  def bottomUpMerging(sortedList: List[(Int, (Set[Int], Double))]):Seq[(Int, Set[Int])] = {
    sortedList.foreach{case (_, (start, _)) => {
        start.foreach { ele:Int => {
          if (!nodeToStart.contains(ele)) {
            nodeToStart += (ele -> startNum)
            startNum += 1
            startToNode += ele
          }
        } 
        }
    }
    }
    ds.extendTo(startNum)
    if (classBased) maxSize = Int.MaxValue
    else 
      maxSize = Math.ceil(startNum.toDouble / partNum).toInt
    
    sortedList.foreach{case (nid, (start, weight)) => {
      var pg: Set[Int] = Set[Int]()
      
      start.foreach { ele:Int => {
        pg += ds.find(nodeToStart(ele))
      }}
      
      var sum: Int = 0
      if (!classBased) pg.foreach { x => sum += ds.getSize(x) }
      if (sum <= maxSize) {
        var now: Int = pg.last
        (pg - now).foreach( ele => {
          ds.union(now, ds.find(ele))
          now = ds.find(now)
        })
      }

      }
    }
    
    val pg: List[(Int, Int)] = ds.getPathGroup().sortWith{ case((k1, v1), (k2, v2)) => {
      v1 > v2
    }}
    
    for (x <- Range(0, partNum)) {
      sortedPartitions.enqueue(x -> 0)
    }
    
    pg.foreach { group => {
      val (p, size) = sortedPartitions.dequeue()
      startToPart += (group._1 -> p)
      sortedPartitions.enqueue(p -> (size + group._2))
    } 
    }

    var nodePartition: Map[Int, Set[Int]] = Map[Int, Set[Int]] ()
    
    sortedList.foreach{case (nid, (start, _)) => {
      var pSet: Set[Int] = Set[Int]()
      start.foreach { ele => pSet += startToPart(ds.find(nodeToStart(ele))) }
      val p = start.map ( x =>  ds.find(nodeToStart(x)) )
      nodePartition += (nid -> pSet)
    }
    }

    nodePartition.toSeq
  }
  
  def getStartGroup(): Map[Int, Int] = {
    var startGroup: Map[Int, Int] = Map[Int, Int] ()
    for (x <- Range(0, startNum)) {
      startGroup += 
        ( startToNode(x) -> startToNode(ds.find(x)) )
    }
    startGroup
  }
  
  def addStartingVertex(x: Int, set: Option[Set[Int]]): Unit = {
    if (!nodeToStart.contains(x)) {
      nodeToStart += (x -> startNum)
      if (set.isEmpty || set.get.size > 1)
      startToPart += (startNum -> -1)
      else startToPart += (startNum -> set.get.last)
      startNum += 1
      if (!classBased) maxSize = Math.ceil(startNum.toDouble/partNum).toInt
      startToNode += x
      ds.extendTo(startNum)
    }
  }
  
  def merge(set: Set[Int]): Boolean = {
    var pg: Set[Int] = Set[Int]()
    set.foreach { ele: Int => {
      pg += ds.find(nodeToStart(ele))
      }}
    var sum: Int = 0
    var partsize: Array[Int] = Array.fill(partNum)(0)
    pg.foreach { x => val size = ds.getSize(x); sum += size; if (startToPart(x) != -1) partsize(startToPart(x)) += size }

    val p: Int = partsize.zipWithIndex.maxBy(_._1)._2
    if (sum <= maxSize) {
        var now: Int = pg.last
        startToPart = startToPart.updated(now, p)
        (pg - now).foreach( ele => {
          ds.union(now, ele)
          startToPart = startToPart.updated(ele, p)
          now = ds.find(now)
        })
        true
      } else {
        println(sum, maxSize)
        false
      }
  }
  
  def fillPart(): Unit = {
    for (x <- Range(0, startNum)) {
      if (ds.array(x) < 0 && startToPart(x) < 0) {
        val (p, size) = sortedPartitions.dequeue()
        startToPart = startToPart.updated(x, p)
        sortedPartitions.enqueue(p -> (size + ds.getSize(x)))
      }
    }
  }
  
  def getPart(x: Int): Int = {
    val ret = startToPart(ds.find(nodeToStart(x)))
    ret
  }
  
  def getGroup(x: Int): Int = {
    startToNode(ds.find(nodeToStart(x)))
  }
  
  def getStartPart(): Map[Int, Int] = {
    var startPart: Map[Int, Int] = Map[Int, Int] ()
    for (x <- Range(0, startNum)) {
      val node = startToNode(x)
      val part = startToPart(ds.find(x))
      startPart += (node -> part)
    }
    startPart
  }
  
}