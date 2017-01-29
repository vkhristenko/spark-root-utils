package org.dianahep.sparkrootutils.apps

import org.apache.spark.sql.SparkSession
import org.dianahep.sparkroot._
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

// tests performance
object PerformanceTester {
  def main(args: Array[String]) {
    if (args.size<5) {println("Improper Number of Arguments"); return}

    // logger
    lazy val logger = LogManager.getLogger("SparkRootPerformanceTester")

    // get the opts
    val maxNumExecs = args(0).toInt
    val numCores = args(1).toInt
    val numTrials = args(2).toInt
    val pathToOutput = args(3);
    val inputFolder = args(4);
    val treeName = 
      if (args.size==6) args(5) else null

    var numExecs = 1;

    while (numExecs <= maxNumExecs) {
      val spark = SparkSession.builder()
        .appName(s"PerformanceTesting_${numExecs}_${numCores}")
        .config("spark.executor.instances", s"$numExecs")
        .config("spark.executor.cores", s"$numCores")
        .getOrCreate()

      logger.info(s"Running numExecs=$numExecs with numCores=${numCores} and defaultParallelism=${spark.sparkContext.defaultParallelism}")
      val df = 
        if (treeName == null)
          spark.sqlContext.read.root(inputFolder)
        else
          spark.sqlContext.read.option("tree", treeName).root(inputFolder)

      for (i <- 0 until numTrials) {
        if (i == numTrials-1) {
          // for the last job - set this job id as the last one
          //spark.sparkContext.setJobGroup("lastJob", "Last Job")
          // set path to the output directory
          spark.sparkContext.setLocalProperty("pathToOutput", 
            s"$pathToOutput")
        }
        logger.info(s"Running trial=$i numExecs=$numExecs with numCores=${numCores}")
        df.count
      }
      
      // stop the session
      spark.stop

      // don't forget to increment
      numExecs += 1
    }
  }
}
