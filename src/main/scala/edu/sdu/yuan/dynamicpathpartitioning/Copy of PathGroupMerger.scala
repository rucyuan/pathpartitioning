package edu.sdu.yuan.dynamicpathpartitioning

import scala.util.control.Breaks._
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ListBuffer

class PathGroupMergerBackup(partitionNum: Int) {
  var startingNum: Int = 0
  var pathgroupSize: Int = 0
  
  var nodeToStart = Map[Int, Int]()
  var startToPart = Map[Int, Int]()
  val startToNode = ListBuffer[Int]()
  
  val sortedPartitions: PriorityQueue[(Int, Int)] = new PriorityQueue[(Int, Int)]()(Ordering.by((t) => -t._2))
  
  val ds: DisjointSet = new DisjointSet(startingNum)
  
  def merge(sortedList: Seq[(Int, Set[Int])], incremental: Boolean = false):Boolean = {
    ds.backup()
    println(startingNum)
    var successful: Boolean = true
    var changedPart: Map[Int, Int] = Map()
    sortedList.foreach{ t => {
      var pg: Set[Int] = Set[Int]()
      
      t._2.foreach { ele:Int => {
        pg += ds.find(nodeToStart(ele))
      }}
      
      var p: Int = 0
      if (incremental) {
        var partsize: Array[Int] = Array.fill(partitionNum)(0)
        pg.foreach { x => val size = ds.getSize(x); if (startToPart(x) != -1) partsize(startToPart(x)) += size }
        p = partsize.zipWithIndex.maxBy(_._1)._2
      }
      var now: Int = pg.last
      (pg - now).foreach( ele => {
          ds.union(now, ds.find(ele))
          now = ds.find(now)
          if (ds.getSize(now) > pathgroupSize) {
            successful = false
            break
          }
        }
      )
      if (!changedPart.contains(now))
      changedPart = changedPart.updated(now, p)
      else changedPart += (now -> p)
      }
    }
    if (!successful) ds.rollback()
    else {
      if (incremental)
      startToPart ++= changedPart
    }
    successful
  }
  
  def assignPartition(): Unit = {
    val list = ds.getPathGroup().sortBy(t => t._2)
    list.foreach(println)
  }
  
  def getStartGroup(): Map[Int, Int] = {
    var startGroup: Map[Int, Int] = Map[Int, Int] ()
    for (x <- Range(0, startingNum)) {
      startGroup += 
        ( startToNode(x) -> startToNode(ds.find(x)) )
    }
    startGroup
  }
  
  def addStartingVertex(x: Int, set: Option[Set[Int]]): Unit = {
    if (!nodeToStart.contains(x)) {
      nodeToStart += (x -> startingNum)
      if (set.isEmpty || set.get.size > 1)
      startToPart += (startingNum -> -1)
      else startToPart += (startingNum -> set.get.last)
      startingNum += 1
      startToNode += x
      ds.extendTo(startingNum)
    }
  }
  
  def merge(set: Set[Int]): Boolean = {
    var pg: Set[Int] = Set[Int]()
    set.foreach { ele: Int => {
      pg += ds.find(nodeToStart(ele))
      }}
    var sum: Int = 0
    var partsize: Array[Int] = Array.fill(partitionNum)(0)
    pg.foreach { x => val size = ds.getSize(x); sum += size; if (startToPart(x) != -1) partsize(startToPart(x)) += size }

    val p: Int = partsize.zipWithIndex.maxBy(_._1)._2
    if (sum <= pathgroupSize) {
        var now: Int = pg.last
        startToPart = startToPart.updated(now, p)
        (pg - now).foreach( ele => {
          ds.union(now, ele)
          startToPart = startToPart.updated(ele, p)
          now = ds.find(now)
        })
        true
      } else {
        println(sum, pathgroupSize)
        false
      }
  }
  
  def fillPart(): Unit = {
    for (x <- Range(0, startingNum)) {
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
    for (x <- Range(0, startingNum)) {
      val node = startToNode(x)
      val part = startToPart(ds.find(x))
      startPart += (node -> part)
    }
    startPart
  }
  
}