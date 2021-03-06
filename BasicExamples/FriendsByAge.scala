package com.snist.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {

  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (age, numFriends)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    // Load each line of the source data into an RDD
    //val lines = sc.textFile("../fakefriends.csv")
    val lines = sc.textFile("F:/SrinidhiProjects/Feb2018/Spark/dataset/friends.csv")
    ////val lines = sc.textFile("file:///root/feb2018data/friends.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)

    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.

    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // totalsByAge.foreach(println)
    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = averagesByAge.collect()

    // Sort and print the final results.
    results.sorted.foreach(println)
  }

}
 /* (30,235)
(31,267)
(32,207)
..........(33,325)
(34,245)
(35,211)
(36,246)
(37,249)
(38,193)
  * 
  * 
  * val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))//totalsByAge.foreach(println)
 .............  (33,(3904,12))
(37,(2244,9))
(23,(2463,10))
(45,(4024,13))
(63,(1536,4))
(67,(3434,16))
(69,(2352,10))
(49,(1108,6))
(31,(2138,8))
(43,(1614,7))
  * 
  *  rdd.mapValues(x => (x, 1))
  * (33,(385,1))
(26,(2,1))
(55,(221,1))
(40,(465,1))
(68,(21,1))
(59,(318,1))
(37,(220,1))
(54,(307,1))
(38,(380,1))
(27,(181,1))
(53,(191,1))
(57,(372,1))
(54,(253,1))
(56,(444,1))
(43,(49,1))
(36,(49,1))
(22,(323,1))
(35,(13,1))
(45,(455,1))
(60,(246,1))
(67,(220,1))
(19,(268,1))
(30,(72,1))
(51,(271,1))
(25,(1,1))
(21,(445,1))
(22,(100,1))
(42,(363,1))
(49,(476,1))
(48,(364,1))
(50,(175,1))
(39,(161,1))
(26,(281,1))
(53,(197,1))
(43,(249,1))
(27,(305,1))
(32,(81,1))
(58,(21,1))
(64,(65,1))
(31,(192,1))
(52,(413,1))
(67,(167,1))
(54,(75,1))
(58,(345,1))
(35,(244,1))
(52,(77,1))
(25,(96,1))
(24,(49,1))
(20,(1,1))
(40,(254,1))
(51,(283,1))
(36,(212,1))
(19,(269,1))
(62,(31,1))
(19,(5,1))
(41,(278,1))
(44,(194,1))
(57,(294,1))
(59,(158,1))
(59,(284,1))
(20,(100,1))
(62,(442,1))
(69,(9,1))
(58,(54,1))
(31,(15,1))
(52,(169,1))
(21,(477,1))
(48,(135,1))
(33,(74,1))
(30,(204,1))
(52,(393,1))
(45,(184,1))
(22,(179,1))
(20,(384,1))
(65,(208,1))
(40,(459,1))
(62,(201,1))
(40,(407,1))
(61,(337,1))
(58,(348,1))
(67,(445,1))
(54,(440,1))
(57,(465,1))
(32,(308,1))
(28,(311,1))
(66,(383,1))
(55,(257,1))
(31,(481,1))
(66,(188,1))
(24,(492,1))
(33,(471,1))
(46,(88,1))
(54,(7,1))
(46,(63,1))
(62,(133,1))
(29,(173,1))
(25,(233,1))
(69,(361,1))
(44,(178,1))
(69,(491,1))
(61,(460,1))
(67,(123,1))
(40,(18,1))
(61,(2,1))
(32,(142,1))
(50,(417,1))
(18,(499,1))
(64,(419,1))
(25,(274,1))
(53,(417,1))
(64,(137,1))
(37,(46,1))
(25,(13,1))
(41,(244,1))
(33,(275,1))
(18,(397,1))
(69,(75,1))
(52,(487,1))
(28,(304,1))
(29,(344,1))
(68,(264,1))
(35,(355,1))
(45,(400,1))
(45,(439,1))
(47,(429,1))
(66,(284,1))
(26,(84,1))
(40,(284,1))
(34,(221,1))
(45,(252,1))
(67,(350,1))
(65,(309,1))
(46,(462,1))
(19,(265,1))
(45,(340,1))
(42,(427,1))
(19,(335,1))
(28,(32,1))
(32,(384,1))
(36,(193,1))
(64,(234,1))
(36,(424,1))
(59,(335,1))
(60,(124,1))
(22,(93,1))
(45,(470,1))
(58,(174,1))
(61,(373,1))
(39,(248,1))
(49,(340,1))
(55,(313,1))
(54,(441,1))
(54,(235,1))
(63,(342,1))
(40,(389,1))
(50,(126,1))
(44,(360,1))
(34,(319,1))
(31,(340,1))
(67,(438,1))
(58,(112,1))
(39,(207,1))
(59,(14,1))
(67,(204,1))
(31,(172,1))
(26,(282,1))
(25,(10,1))
(48,(57,1))
(68,(112,1))
(53,(92,1))
(68,(490,1))
(29,(126,1))
(55,(204,1))
(23,(129,1))
(47,(87,1))
(38,(459,1))
(55,(474,1))
(67,(316,1))
(26,(381,1))
(37,(426,1))
(30,(108,1))
(43,(404,1))
(26,(145,1))
(47,(488,1))
(44,(84,1))
(48,(287,1))
(31,(109,1))
(47,(225,1))
(54,(369,1))
(62,(23,1))
(60,(294,1))
(40,(349,1))
(45,(497,1))
(60,(125,1))
(38,(2,1))
(30,(376,1))
(38,(173,1))
(38,(76,1))
(48,(381,1))
(38,(180,1))
(21,(472,1))
(23,(174,1))
(63,(469,1))
(46,(125,1))
(64,(164,1))
(69,(236,1))
(21,(491,1))
(41,(206,1))
(37,(271,1))
(27,(174,1))
(33,(245,1))
(61,(73,1))
(55,(284,1))
(28,(312,1))
(32,(182,1))
(22,(6,1))
(34,(116,1))
(29,(260,1))
(66,(350,1))
(26,(345,1))
(41,(394,1))
(27,(150,1))
(34,(346,1))
(40,(406,1))
(44,(277,1))
(19,(106,1))
(37,(207,1))
(40,(198,1))
(26,(293,1))
(24,(150,1))
(54,(397,1))
(59,(42,1))
(68,(481,1))
(67,(70,1))
(49,(22,1))
(57,(8,1))
(62,(442,1))
(61,(469,1))
(25,(305,1))
(48,(345,1))
(46,(154,1))
(45,(332,1))
(25,(101,1))
(61,(68,1))
(21,(471,1))
(28,(174,1))
(41,(260,1))
(52,(338,1))
(21,(138,1))
(66,(41,1))
(36,(342,1))
(55,(57,1))
(36,(174,1))
(69,(116,1))
(67,(79,1))
(60,(324,1))
(32,(412,1))
(51,(161,1))
(68,(217,1))
(29,(11,1))
(38,(96,1))
(40,(172,1))
(51,(334,1))
(40,(33,1))
(29,(228,1))
(27,(471,1))
(66,(496,1))
(49,(106,1))
(26,(298,1))
(55,(289,1))
(44,(353,1))
(25,(446,1))
(29,(367,1))
(51,(493,1))
(64,(244,1))
(47,(13,1))
(54,(462,1))
(46,(300,1))
(44,(499,1))
(23,(133,1))
(26,(492,1))
(21,(89,1))
(32,(404,1))
(65,(443,1))
(26,(269,1))
(43,(101,1))
(30,(384,1))
(64,(396,1))
(56,(354,1))
(30,(221,1))
(62,(290,1))
(23,(373,1))
(63,(380,1))
(23,(65,1))
(38,(410,1))
(40,(56,1))
(38,(454,1))
(45,(395,1))
(57,(207,1))
(57,(311,1))
(49,(147,1))
(28,(108,1))
(37,(263,1))
(46,(319,1))
(19,(404,1))
(29,(182,1))
(23,(323,1))
(41,(340,1))
(45,(59,1))
(67,(153,1))
(68,(189,1))
(43,(48,1))
(61,(421,1))
(59,(169,1))
(36,(168,1))
(25,(208,1))
(64,(391,1))
(59,(439,1))
(35,(251,1))
(30,(476,1))
(62,(450,1))
(44,(61,1))
(58,(92,1))
(29,(236,1))
(56,(343,1))
(51,(492,1))
(46,(407,1))
(20,(63,1))
(62,(41,1))
(67,(35,1))
(33,(356,1))
(30,(17,1))
(55,(362,1))
(29,(207,1))
(40,(7,1))
(27,(337,1))
(47,(4,1))
(58,(10,1))
(28,(180,1))
(66,(305,1))
(57,(275,1))
(18,(326,1))
(46,(151,1))
(26,(254,1))
(30,(487,1))
(31,(394,1))
(29,(329,1))
(32,(24,1))
(33,(460,1))
(20,(277,1))
(55,(464,1))
(54,(72,1))
(27,(53,1))
(64,(499,1))
(69,(15,1))
(46,(352,1))
(67,(149,1))
(26,(7,1))
(52,(276,1))
(54,(442,1))
(39,(68,1))
(68,(206,1))
(39,(120,1))
(41,(397,1))
(54,(115,1))
(65,(430,1))
(19,(119,1))
(39,(106,1))
(26,(383,1))
(48,(266,1))
(53,(86,1))
(31,(435,1))
(62,(273,1))
(19,(272,1))
(68,(293,1))
(66,(201,1))
(23,(392,1))
(18,(418,1))
(47,(97,1))
(60,(304,1))
(35,(65,1))
(38,(95,1))
(66,(240,1))
(69,(148,1))
(67,(355,1))
(57,(436,1))
(35,(428,1))
(43,(335,1))
(30,(184,1))
(38,(38,1))
(22,(266,1))
(64,(309,1))
(64,(343,1))
(50,(436,1))
(23,(230,1))
(56,(15,1))
(67,(38,1))
(69,(470,1))
(26,(124,1))
(24,(401,1))
(29,(128,1))
(42,(467,1))
(58,(98,1))
(21,(224,1))
(18,(24,1))
(56,(371,1))
(57,(121,1))
(36,(68,1))
(62,(496,1))
(19,(267,1))
(35,(299,1))
(58,(22,1))
(53,(451,1))
(45,(147,1))
(56,(313,1))
(30,(65,1))
(33,(294,1))
(37,(106,1))
(32,(212,1))
(55,(176,1))
(26,(391,1))
(40,(261,1))
(67,(292,1))
(44,(388,1))
(55,(470,1))
(33,(243,1))
(24,(77,1))
(28,(258,1))
(68,(423,1))
(63,(345,1))
(36,(493,1))
(36,(343,1))
(45,(54,1))
(38,(203,1))
(57,(289,1))
(42,(275,1))
(57,(229,1))
(59,(221,1))
(42,(95,1))
(18,(417,1))
(48,(394,1))
(38,(143,1))
(46,(105,1))
(64,(175,1))
(18,(472,1))
(40,(286,1))
(32,(41,1))
(38,(34,1))
(48,(439,1))
(52,(419,1))
(37,(234,1))
(28,(34,1))
(58,(6,1))
(44,(337,1))
(52,(456,1))
(33,(463,1))
(37,(471,1))
(51,(81,1))
(44,(335,1))
(26,(84,1))
(47,(400,1))
(41,(236,1))
(23,(287,1))
(40,(220,1))
(25,(485,1))
(53,(126,1))
(33,(228,1))
(42,(194,1))
(46,(227,1))
(55,(271,1))
(38,(160,1))
(52,(273,1))
(27,(154,1))
(35,(38,1))
(34,(48,1))
(52,(446,1))
(28,(378,1))
(50,(119,1))
(41,(62,1))
(44,(320,1))
(43,(428,1))
(32,(97,1))
(48,(146,1))
(57,(99,1))
(22,(478,1))
(47,(356,1))
(49,(17,1))
(69,(431,1))
(61,(103,1))
(33,(410,1))
(65,(101,1))
(60,(2,1))
(19,(36,1))
(23,(357,1))
(18,(194,1))
(46,(155,1))
(39,(275,1))
(34,(423,1))
(62,(36,1))
(62,(12,1))*/
