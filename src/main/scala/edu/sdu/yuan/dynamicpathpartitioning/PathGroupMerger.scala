package edu.sdu.yuan.dynamicpathpartitioning

import scala.util.control.Breaks._
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ListBuffer

class PathGroupMerger(partitionNum: Int) {
  var pathgroupSize: Int = 0
  
  var nodeNum: Int = 0
  
  var startingNum: Int = 0
  
  var nodePartition: Map[Int, Int] = Map[Int, Int]() 
  
  var starting: Array[Boolean] = Array.emptyBooleanArray
  
  val sortedPartitions: PriorityQueue[(Int, Int)] = new PriorityQueue[(Int, Int)]()(Ordering.by((t) => -t._2))
  
  val ds: DisjointSet = new DisjointSet(nodeNum)
  
  def extendTo(newsize: Int): Unit = {
    if (newsize > nodeNum) {
      starting ++= Array.fill(newsize - nodeNum)(false)
      ds.extendTo(newsize)
      nodeNum = newsize
    }
  }
  
  def setStartingNum(newnum: Int): Unit = {
    startingNum = newnum
    pathgroupSize = Math.ceil(startingNum.toDouble / partitionNum).toInt
  }
    
  def refreshStartingNum(): Unit = {
    startingNum = starting.count(_ == true)
    pathgroupSize = Math.ceil(startingNum.toDouble / partitionNum).toInt
  }
  
  def merge(sortedList: Seq[(Int, Set[Int])], incremental: Boolean = false):Boolean = {
    ds.backup()
    var successful: Boolean = true
    var changedPart: Map[Int, Int] = Map[Int, Int]()
    breakable {
    sortedList.foreach( t => t._2.foreach { x => starting(x) = true })
    sortedList.foreach{ t => {
      var pathGroupSet: Set[Int] = Set[Int]()
      
      t._2.foreach { st:Int => {
        pathGroupSet += ds.find(st)
      }}
      
      var p: Int = 0
      if (incremental) {
        var partitionSizeDis: Array[Int] = Array.fill(partitionNum)(0)
        pathGroupSet.foreach ( pg => { 
          val size = ds.getSize(pg) 
          if (nodePartition.contains(pg) && nodePartition(pg) >= 0)
            partitionSizeDis(nodePartition(pg)) += size
          } )
        p = partitionSizeDis.zipWithIndex.maxBy(_._1)._2
      }
        
      var currentPg: Int = pathGroupSet.last
      
      (pathGroupSet - currentPg).foreach( pg => {
          ds.union(currentPg, pg)
          currentPg = ds.find(currentPg)
          if (ds.getSize(currentPg) > pathgroupSize) {
            successful = false
            break
          }
        }
      )
      
      if (changedPart.contains(currentPg))
      changedPart = changedPart.updated(currentPg, p)
      else changedPart += (currentPg -> p)
      }
    }
    }
    if (!successful) ds.rollback()
    else {
      if (incremental) nodePartition ++= changedPart
    }
    successful
  }
  
  def assignPartition(): Unit = {
    for (i <- Range(0, partitionNum)) {
      sortedPartitions.enqueue((i,0))
    }
    val list = ds.getPathGroup().sortBy(t => t._2)(scala.math.Ordering.Int.reverse)
    list.foreach( t => 
      if (starting(t._1)) {
        val (part, size) = sortedPartitions.dequeue()
        nodePartition += (t._1 -> part)
        sortedPartitions.enqueue((part,size + t._2))
      }
    )
    fillPartition
  }
  
  def fillPartition(): Unit = {
    for (i <- Range(0, nodeNum)) if (starting(i)) {
      if (!nodePartition.contains(i))
      nodePartition += (i -> nodePartition(ds.find(i)))
      else nodePartition = nodePartition.updated(i, nodePartition(ds.find(i)))
    }
  }
  
  def addStartingVertex(x: Int, set: Option[Set[Int]]): Unit = {
    if (!starting(x)) {
      ds.array(x) = -1
      starting.update(x, true)
      if (set.isEmpty || set.get.size > 1)
      nodePartition += (x -> -1)
      else nodePartition += (x -> set.get.last)
    }
  }
  
  def getStartGroup(): Map[Int, Int] = {
    var startGroup: Map[Int, Int] = Map[Int, Int]()
    for (i <- Range(0, starting.length)) if (starting(i))
      startGroup += (i -> ds.find(i))
    startGroup
  }
}