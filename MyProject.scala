
    /**************************************************
     * Note: In Maven dependency                      *
     * Spark and Spark SQL must have the same version * 
     * This code is created using 2.11 version 1.6.0  *
    ***************************************************/
import java.io._
import scala.io.Source
import scala.math.random
import org.apache.spark.SparkContext 
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import au.com.bytecode.opencsv._
object SparkCore extends App{ 

override def  main(args: Array[String]):Unit = {
   val conf = new SparkConf().setAppName("MyProject").setMaster("local[1]").set("spark.executor.memory","1g");
		
		//create spark context object 
		val sc = new SparkContext(conf)
   

   /***************************************************
     *                                                *
     * IF YOU ARE USING TERMINAL MODE NOT AN IDE      * 
     * START HERE.    					      *
     *                                                *
    ***************************************************/


    
    sc.setLogLevel("WARN")
   val csv = sc.textFile("creditCard.csv")
            // Create a Spark DataFrame
    val header = csv.first()
    val creditCardData = csv.filter(line=>line != header)
    val headerAndRows  = creditCardData.map(line => line.split(",").map(_.trim))
   //val ser=sc.parallelize(headerAndRows)
    val Population = headerAndRows
     .map(p => ( p(9).toInt, p(4).toDouble))
    Population.foreach(line=>println(line))
    val MyGroupedPopulation=Population.groupByKey()
 //   MyGroupedPopulation.foreach(line=>println(line))
    //step 3
  def mean(list:List[Double]):Double={
    list.reduce(_+_).toDouble/list.size
  }
  def variance(list:List[Double]):Double={
    var m:Double=mean(list)
    var variance:Double= 
    list.map(x=>(x-m)*(x-m)).reduce(_+_).toDouble/list.size
    variance
  }
def calculatVarianceMean(data:(Int,Iterable[Double])):(Int,Double,Double)={
  val key = data._1
  val iterator = data._2.toList
  (key,mean(iterator),variance(iterator))
}
def calculatVarianceMeanList(data:(Int,List[Double])):(Int,Double,Double)={
  val key = data._1
  val iterator = data._2.toList
  (key,mean(iterator),variance(iterator))
}
def createSampleWith(list: List[(Int,Double)],frc:Double,withRep:Boolean): List[(Int,Double)] = {
  sc.parallelize(list).sample(withRep,frc).collect().toList
}
def BootsrtapingNewResult(data:(List[Double])):(Double,Double)={
  val newList=data.toList
  var mea=mean(newList.toList)
  var vari=variance(newList.toList)
  (mea,vari)
}
val PopulationToSamle=MyGroupedPopulation
val result=MyGroupedPopulation.map(p=>calculatVarianceMean(p)).sortBy(_._1, true)
println("\t\tCategory\t\tMean\t\t\t\tVariance")
result.foreach(line=>println("\t\t"+line._1+"\t\t"+line._2+"\t\t"+line._3))
//create sample and compute
val population25Pecent=createSampleWith(Population.collect().toList,0.65,false)
def generateResult(list:List[(Int,Double)],loopTime:Int):Unit={
  var result:List[(Int,(Double,Double))] = List()
  for(i<-0 until loopTime){
  val population100Pecent=createSampleWith(list,1.0,true)
  sc.parallelize(population100Pecent).groupByKey().map(m=>(m._1,BootsrtapingNewResult(m._2.toList)))
  .collect().foreach(f=>{
   
  result=f::result})
  }
  println(result.size)
   val fin= sc.parallelize(result).mapValues(m=>(m._1,m._2))
   .reduceByKey((x,y)=>((x._1+y._1)/loopTime,(x._2+y._2)/loopTime)).sortBy(_._1, true)
   println("\t\tCategory\t\tMean\t\t\t\tVariance")
   fin .foreach(line=>{
      
      println("\t\t"+line._1+"\t\t"+line._2._1+"\t\t"+line._2._2)
    })
    println(" Compute the error value for calculation\n")
    println("\t\tPercentage\t\tCategory\t\tMean Error\t\t\t\tVariance Erro")
    fin.foreach{ case (key, value) => 
     val rs= result(key)
      println("\t\t25"+"\t\t\t"+key+"\t\t"+Math.abs(value._1-rs._2._1)/value._1+
      "\t\t"+Math.abs(value._2-rs._2._2)/value._2)

    }

}
generateResult(population25Pecent,2)
 
  sc.stop
}
}