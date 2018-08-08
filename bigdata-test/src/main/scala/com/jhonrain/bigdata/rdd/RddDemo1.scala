package com.jhonrain.bigdata.rdd

import org.apache.spark.{ SparkContext, SparkConf }

object RddDemo1 {
  def main(args: Array[String]) {
    var conf = new SparkConf();
	  conf.setAppName("rdddemo1");
	  conf.setMaster("local[2]");
	  var sc = new SparkContext(conf);
	  /**
	   * 使用makeRDD创建RDD,可以通过List或者Array来创建
	   * 
	   */
	  val rdd0 = sc.makeRDD(List(1,2,3,4,5,6));
	  val rdd1 = sc.makeRDD(Array(111,2222,333))
	  val rdd2 = rdd0.map{x => x*x }
	  println(rdd2.collect().mkString(","))
	  
	  /**
	   * 使用parallelize创建rdd
	   */
	  val rdd3 = sc.parallelize(List(3,7,5),1)
	  val rdd4 = rdd3.map{x=>x+x}
	  println(rdd4.collect().mkString("-"));
	  
	  
	  // rdd本质上就是
	  
	  val lines = sc.textFile("E:\\scala-workspace\\bigdata-test\\src\\main\\scala\\com\\jhonrain\\bigdata\\rdd\\demo1")
	  val read = lines.flatMap{x => x.split("，")}
	  println(read.collect().mkString("--"))
  }
}