package com.snist.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount");

    //val input = sc.textFile("F:/SrinidhiProjects/SparkScala/SparkScala/spark.txt");
    
    val input = sc.textFile("F:/SrinidhiProjects/Feb2018/Spark/dataset/spark.txt");
    //val input = sc.textFile("file:///root/feb2018data/spark.txt");

    //val words = input.flatMap(x => x.split("\\W+"));
    val words = input.flatMap(x => x.split("\\s"));

    val wordsCount = words.countByValue();

    wordsCount.foreach(println)
    
   /* //Most popular word
    
    val wordsCountLC = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordsSum = wordsCountLC.map(x => (x,1)).reduceByKey((x,y) => x+y)
    
    //(w,cnt) => (cnt,w) =>then sort
    val wordsSumFlip = wordsSum.map(x => (x._2,x._1)).sortByKey(false);
    
    wordsSumFlip.foreach(println)*/
    
    

  }

}

  /*// Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // Read each line of my book into an RDD
   // val input = sc.textFile("../book.txt")
    val input = sc.textFile("F:/SrinidhiProjects/SparkScala/SparkScala/book.txt");
    
    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))
    
    // Count up the occurrences of each word
    val wordCounts = words.countByValue()
    
    // Print the results.
    wordCounts.foreach(println)*/

/*output:
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties

[Stage 0:>                                                          (0 + 0) / 2]
                                                                                (University,1)
(parallelism,1)
(for,1)
(Apache,2)
(is,1)
(since.,1)
(donated,1)
(clusters,1)
(Software,1)
(California,,1)
(programming,1)
(data,1)
(interface,1)
(it,1)
(entire,1)
(provides,1)
(has,1)
(Berkeley's,1)
(to,1)
(maintained,1)
(was,1)
(at,1)
(AMPLab,,1)
(implicit,1)
(framework.,1)
(Originally,1)
(developed,1)
(cluster-computing,1)
(with,1)
(Spark,3)
(which,1)
(an,2)
(fault,1)
(Foundation,,1)
(codebase,1)
(tolerance.,1)
(open-source,1)
(of,1)
(and,1)
(later,1)
(the,3)

*/

/*Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties

[Stage 0:>                                                          (0 + 0) / 2]
                                                                                (3,spark)
(3,the)
(2,apache)
(2,an)
(1,university)
(1,provides)
(1,is)
(1,maintained)
(1,california,)
(1,codebase)
(1,with)
(1,developed)
(1,later)
(1,since.)
(1,data)
(1,framework.)
(1,parallelism)
(1,open-source)
(1,originally)
(1,has)
(1,fault)
(1,amplab,)
(1,interface)
(1,it)
(1,implicit)
(1,was)
(1,berkeley's)
(1,entire)
(1,to)
(1,at)
(1,tolerance.)
(1,cluster-computing)
(1,which)
(1,of)
(1,clusters)
(1,foundation,)
(1,for)
(1,software)
(1,programming)
(1,and)
(1,donated)*/

