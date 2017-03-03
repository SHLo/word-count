package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCounter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Counter")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("s3://ce-global-dmp/temporary/tags.csv")
    val words = textFile.flatMap(_.split("\t"))
    val result = words.map((_, 1)).reduceByKey(_+_)

    println(result)
    //result.saveAsTextFile("s3://ce-global-dmp/temporary/tags.csv.out")
  }
}
