package edu.sdu.yuan.pathpartitioning

class DisjointSet(len: Int) {
  var size: Int = len
  var array: Array[Int] = Array.fill(size)(-1)
  var copyArray: Array[Int] = new Array[Int](size)
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
  def extendTo(len: Int) = {
    if (len > size) {
      val aug = len - size
      size = len
      array ++= Array.fill(aug)(-1)
      copyArray ++= Array.fill(aug)(0)
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
  def backup(): Unit = {
    Array.copy(array, 0, copyArray, 0, array.size)
  }
  def recover(): Unit = {
    Array.copy(copyArray, 0, array, 0, array.size)
  }
}