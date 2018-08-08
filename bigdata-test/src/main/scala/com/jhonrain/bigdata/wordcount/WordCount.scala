package com.jhonrain.bigdata.wordcount

import org.apache.spark.{ SparkContext, SparkConf }

object WordCount {
  def main(args:Array[String]){
	  /**
	   * 第一步：创建spark的配置对象sparkconf,设置spark程序运行时的配置信息。
	   * setAppName用好设置应用程序的名称，在程序运行的监控界面可以看到该名称。
	   * setMaster：设置程序运行在本地还是运行在集群环境中，在本地可以使用local参数，或者local[k]
	   * 如果运行在集群中，以standalone模式运行呢，需要使用spark://Host:Port
	   */
	  var conf = new SparkConf();
	  conf.setAppName("wordcount");
	  conf.setMaster("local[2]");
	  
	  /**
	   * 第二步：创建sc对象，sc是spark程序所有功能的唯一入口。
	   * sc核心作用:初始化spark应用程序所需要的核心组件，包括DAGScheduler TaskScheduler *SchedulerBackend
	   * 还会负责spark程序向master注册程序
	   */
	  var sc = new SparkContext(conf);
	  
	  /**
	   * 第三步：根据具体的数据来源等通过sparkcontext来创建rdd;
	   * rdd创建方式：外部来源
	   * 通过scala的集合使用，然后产生rdd,
	   * 通过rdd生成rdd
	   */
	  val input = sc.textFile("C:\\Users\\jiangy19\\Desktop\\sparkwordcount.txt");
	  /**
	   * 第四步：使用一些函数来进行计算 
	   */
	  val words = input.flatMap(_.split("，")).flatMap(_.split(" ")).filter(word=>word!=" ");
	  
	  val pairs = words.map(word => (word, 1))
	  val wordcounts = pairs.reduceByKey ( _+_ )
	  val result = wordcounts.collect();
	  result.foreach(println);
	  /** 释放资源 **/
	  sc.stop();
  }
}