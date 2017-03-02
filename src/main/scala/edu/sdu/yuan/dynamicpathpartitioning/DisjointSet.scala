package edu.sdu.yuan.dynamicpathpartitioning

class DisjointSet(len: Int) {
  var size: Int = len
  var array: Array[Int] = Array.fill(size)(-1)
  var newArray: Array[Int] = null
  
  def union(root1: Int, root2: Int): Unit = {
		if (root1 == root2) return;
	    if (array.apply(root2) < array.apply(root1)) {
	    	array.apply(root2) += array.apply(root1);
	    	array.update(root1, root2);
	    } else {
	      array.apply(root1) += array.apply(root2);
	      array.update(root2, root1);
	    }
	  }
  
  def backup() {
    newArray = new Array[Int](array.size)
    Array.copy(array, 0, newArray, 0, array.size)
  }
  
  def rollback() {
    Array.copy(newArray, 0, array, 0, array.size)
  }
  
  def extendTo(len: Int) = {
    if (len > size) {
      val aug = len - size
      size = len
      array ++= Array.fill(aug)(-1)
    }
  }
  def find(x: Int):Int = {
	    if (array.apply(x) < 0) {
	      x
	    } 
	    else
	    {
	      array.update(x, find(array.apply(x)));
	      array.apply(x)
	    }
	  }
  
  def getSize(x: Int): Int = {
    val root: Int = find(x)
    -array.apply(root)
  }
  
  def getPathGroup(): List[(Int, Int)] = {
    var pg: List[(Int, Int)] = List[(Int, Int)] ()
    var i = 0
    while (i < size) {
      if (array.apply(i) < 0) {
        pg ::= (i, -array.apply(i))
      }
      i += 1
    }
    pg
  }
  
}