package edu.sdu.yuan.pathpartitioning

import scala.util.control.Breaks._
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

object BottomUpMerging {
  def bottomUpMerging(sortedList: Array[(Int, (Set[Int], Double))], k: Int): Seq[(Int, Set[Int])] = {
    val nodeToStart = Map[Int, Int]()
    val startToNode = ListBuffer[Int]()
    var startNum: Int = 0
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
    val maxSize: Int = Math.ceil(startNum*1.0 / k).toInt
    val ds = new DisjointSet(startNum)
    sortedList.foreach{case (_, (start, _)) => {
      ds.backup()
      val first = nodeToStart.apply(start.head)
      breakable {
      start.tail.foreach { ele:Int => {
        ds.union(ds.find(first), ds.find(nodeToStart.apply(ele)))
        if (ds.getSize(first) > maxSize) {
          ds.recover()
          break
        }
      } 
      }
      }
    }
    }
    
    val startToSet: Map[Int, scala.collection.mutable.Set[Int]] = Map[Int, scala.collection.mutable.Set[Int]]()
    for (x <- Range(0, startNum)) {
      val group = ds.find(x)
      if (!startToSet.contains(group)) {
        startToSet += (group -> scala.collection.mutable.Set(startToNode.apply(x)))
      } else {
        startToSet.apply(group) += startToNode.apply(x)
      }
    }
    
    val sortedSetList: List[Set[Int]] = startToSet.toList.map{case (_, set) => {
      set.toSet
    }
    }.sortWith{ case (left, right) => left.size > right.size}
    
    val sortedPartitions: PriorityQueue[(Int, Int)] = new PriorityQueue[(Int, Int)]()(Ordering.by((t) => -t._2))
    for (x <- Range(0, k)) {
      sortedPartitions.enqueue(x -> 0)
    }
    sortedSetList.foreach { set => {
      val (p, size) = sortedPartitions.dequeue()
      set.foreach { ele => nodeToStart.update(ele, p) }
      sortedPartitions.enqueue(p -> (size + set.size))
    } 
    }
    val nodePartition: Map[Int, Set[Int]] = Map[Int, Set[Int]] ()
    sortedList.foreach{case (nid, (start, _)) => {
      val pSet: scala.collection.mutable.Set[Int] = scala.collection.mutable.Set[Int]()
      start.foreach { ele => pSet += nodeToStart.apply(ele) }
      nodePartition += (nid -> pSet.toSet)
    }
    }
    nodePartition.toSeq
  }
}