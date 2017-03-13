package edu.sdu.yuan.dynamicpathpartitioning

import org.apache.spark._
import org.apache.spark.broadcast._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.control.Breaks._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._

object PathPartApp{
  type OptionMap = Map[Symbol, Any]
  
  def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--part-num" :: value :: tail =>
                               nextOption(map ++ Map('partnum -> value.toInt), tail)
        case "--dict" :: value :: tail =>
                               nextOption(map ++ Map('dict -> value), tail)
        case "--alpha" :: value :: tail =>
                               nextOption(map ++ Map('alpha -> value.toDouble), tail)
        case "--sigma" :: value :: tail =>
                               nextOption(map ++ Map('sigma -> value.toDouble), tail)                       
        case "--iter" :: value :: tail =>
                               nextOption(map ++ Map('iter -> value.toInt), tail)
        case "--ins" :: value :: tail => 
                               nextOption(map ++ Map('ins -> value.toDouble), tail)
        case "--del" :: value :: tail => 
                               nextOption(map ++ Map('del -> value.toDouble), tail)
        case "--evltime" :: value :: tail =>
                               nextOption(map ++ Map('evltime -> value.toInt), tail)
        case string :: opt2 :: tail if isSwitch(opt2) => 
                               nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail => println("Unknown option "+option) 
                               null
      }
    }
  def main(args: Array[String]): Unit = {
    val usage: String = """
        Usage: PathPartitioning [--part-num PartitionNumber] [--alpha ConvergenceDecayFactor] [--iter WeightingIterationNumber] [--sigma PartitionRelaxationFactor] [--ins InsertionRatio] [--del DeletionRatio] [--evltime EvolutionTime] foldername
      """
    if (args.length == 0) println(usage)
    val arglist = args.toList
    
    val options = nextOption(Map(),arglist)

    val partitionNum: Int = options.get('partnum).getOrElse(4).asInstanceOf[Int]
    val foldername: String = options.get('infile).get.asInstanceOf[String]
    val insertionRatio: Double = options.get('ins).getOrElse(0.0).asInstanceOf[Double]
    val deletionRatio: Double = options.get('del).getOrElse(0.0).asInstanceOf[Double]
    val evolutionTime: Int = options.get('evltime).getOrElse(1).asInstanceOf[Int]
    val alpha: Double = options.get('alpha).getOrElse(0.8).asInstanceOf[Double]
    val sigma: Double = options.get('sigma).getOrElse(0).asInstanceOf[Double]
    val iterNum: Int = options.get('iter).getOrElse(5).asInstanceOf[Int]
    val pw = new PrintWriter(new BufferedWriter(new FileWriter(foldername.split("/").last+".result")))
    pw.write("Method Name,Number,Running Time,SD(σ),Max Partition Size,Data Duplication,Merged Vertices Number,Merged Classes Number,Triples Movement Number\n")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val conf = new SparkConf().setAppName("PathPartitioning").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val feeder: DataFeeder = new DataFeeder(sc, insertionRatio, deletionRatio, evolutionTime, foldername+"/input", foldername+"/dict")
    val outputname: String = "file:///home/yuan/Path/"+foldername.split("/").last

    val t0 = System.nanoTime()
    val ppp = new PathPartitioningPlan(sc, feeder, partitionNum)
    InitPathPartitioner.initializePPP(ppp, alpha, iterNum)
    val t1 = System.nanoTime()
    printStatistics(pw, -1, "Static Method", ppp, (t1-t0)/1e6.toLong)
    PathPartitioningPlan.printN3LocalFiles(ppp, feeder, outputname+"/init")
    for (iteration <- Range(0, evolutionTime)) {
      val t0 = System.nanoTime()
      IncPathPartitioner.maintainPPP(ppp, feeder)
      val t1 = System.nanoTime()
      //PathPartitioningPlan.printN3Files(ppp, feeder, foldername+"/output(inc)"+iteration)
      printStatistics(pw, iteration, "Incremental Method", ppp, (t1-t0)/1e6.toLong)

      val t2 = System.nanoTime()
      val pp = new PathPartitioningPlan(sc, feeder, partitionNum, iteration)
      InitPathPartitioner.initializePPP(pp, alpha, iterNum)
      val t3 = System.nanoTime()      
      //PathPartitioningPlan.printN3Files(pp, feeder, foldername+"/output(sta)"+iteration)
      
      printStatistics(pw, iteration, "Static Method", pp, (t3-t2)/1e6.toLong)
      
    }
    pw.close()
    }
  def printStatistics(pw: PrintWriter, iteration: Int, methodName: String, ppp: PathPartitioningPlan, time: Long): Unit = {
      val loadBalance = ppp.loadBalance().map(t => t._2)
      val sum = loadBalance.sum
      val count = loadBalance.size
      val min = loadBalance.min
      val max = loadBalance.max
      val normalized = loadBalance.map { x => (x - min).toDouble / (max - min).toDouble }
      val mean = normalized.sum / count
      val devs = normalized.map { x => (x - mean) * (x - mean) }
      val sd = devs.sum / count
      val total = ppp.triples.count()
      val merged = (ppp.mergedVerticeNum, ppp.mergedClassesNum)
      pw.print(methodName+",#"+(iteration+1)+","+time/6e4.toLong+"min"+time/1e3.toLong%60+"s"+time%1e3.toLong+"ms,"+sd+","
          +(max.toDouble/sum*10000).toLong.toDouble/100+"%,"
          +(sum-total).toDouble/total+","+merged._1+","+merged._2+","+ppp.dataMovement+"\n")
      pw.flush()
  }
}