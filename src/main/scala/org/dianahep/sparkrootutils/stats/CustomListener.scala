package org.dianahep.sparkrootutils.stats

// import spark deps
import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListener, SparkListenerJobStart, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerStageSubmitted, SparkListenerTaskStart, SparkListenerTaskGettingResult, SparkListenerTaskEnd, SparkListenerExecutorAdded, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved}

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import scala.collection.mutable.ListBuffer
import java.io._

// custom lister
class CustomListener extends SparkListener {
  // logger
  lazy val logger = LogManager.getLogger("SparkRootCustomListener")

  case class Job(var id: Int, var startTime: Long, var endTime: Long);
  var appId = "";
  var appName = "";
  val jobs: ListBuffer[Job] = ListBuffer.empty[Job]
  var currentJob: Job = null
  var pathToOutput = ""

  // application start/end
  override def onApplicationEnd(appEnd: SparkListenerApplicationEnd) {
    logger.info(s"Application ended $appEnd")

    logger.info(s"Dumping Statistics")
    logger.info(s"${appId} ${appName}")

    // open to append
    val f = new File(pathToOutput)
    val writter = new FileWriter(f, 
      if (f.exists) true else false)
    for (x <- jobs) 
      writter.write(s"$appName,$appId,${x.id},${x.startTime},${x.endTime}\n")
    writter.close
  }

  override def onApplicationStart(appStart: SparkListenerApplicationStart) {
    logger.info(s"Application started $appStart")
    appStart.appId match {
      case Some(id) => appId = id
      case None => appId = "nullId"
    }
    appName = appStart.appName
  }

  // job start/end
  override def onJobStart(jobStart: SparkListenerJobStart) {
    logger.info(s"Job started with ${jobStart.stageInfos.size} stages: $jobStart properties: ${jobStart.properties}")
    currentJob = Job(jobStart.jobId, jobStart.time, 0)

    pathToOutput = jobStart.properties.getProperty("pathToOutput", "")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    logger.info(s"job ended $jobEnd")
    currentJob.endTime = jobEnd.time
    jobs += currentJob
  }

  // stage submitted/completed
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.info(s"Stage ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks.")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = {
    logger.info(s"Stage submitted $stageSubmitted")
  }

  // task start/end/gettingResult
  override def onTaskStart(taskStart: SparkListenerTaskStart) = {
    logger.info(s"Task started $taskStart")
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) = {
    logger.info(s"Task Getting Result $taskGettingResult")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = {
    logger.info(s"Task Edn $taskEnd")
  }


  // executor added/reomved/metrics
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) = {
    logger.info(s"Executor added $executorAdded")
  }

  /*
  override def onExecutorMetricsUpdate(
          executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) = {
    println(s"Executor Metrics Update $executorMetricsUpdate")
  }*/

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) = {
    logger.info(s"Executor Removed $executorRemoved")
  }
}
