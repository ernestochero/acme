import java.io.File

import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path }
import org.apache.spark.SparkContext
import Pattern._
import scala.util.matching.Regex

object AcmeModule {
  private def readHDFSDirectory(
    strPath: String
  )(implicit sparkContext: SparkContext): Option[List[FileStatus]] = {
    val directoryPath = new Path(strPath)
    val fileSystem    = FileSystem.get(sparkContext.hadoopConfiguration)
    if (fileSystem.isDirectory(directoryPath))
      Some(fileSystem.listStatus(directoryPath).toList)
    else None
  }
  private def processHDFSDirectory(stage: List[FileStatus], pattern: Regex): List[Option[String]] =
    stage.map(_.getPath.getName).map(name => pattern findFirstIn name)

  private def readDirectory(path: String, filePatterns: String = ""): Option[File] = {
    val file = new File(path)
    if (file.isDirectory) Some(file)
    else None
  }

  private def processDirectory(stage: File, pattern: Regex): List[Option[String]] = {
    val files = stage.listFiles().map(_.getName)
    files.map(name => pattern findFirstIn name).toList
  }

  private def validateDirectories(
    stagingDirectory: List[Option[String]],
    rawDirectory: List[Option[String]],
    masterDirectory: List[Option[String]]
  ): Boolean = {
    val stagingCount = stagingDirectory.count(_.isDefined)
    val rawCount     = rawDirectory.count(_.isDefined)
    val masterCount  = masterDirectory.count(_.isDefined)
    stagingCount == rawCount &&
    rawCount == masterCount
  }

  private def createStageObject(csvLine: String): Stage = {
    val lineContent = csvLine.split(",").map(_.trim)
    Stage(lineContent(0), lineContent(1), lineContent(2))
  }

  def readCSV(csvPath: String): List[Stage] = {
    import scala.io._
    val csvFile = Source.fromFile(csvPath)
    val csvContent = csvFile
      .getLines()
      .map(createStageObject)
      .toList
    csvContent
  }

  private def executeProcessHDFS(stagingPath: String, rawPath: String, masterPath: String)(
    implicit sparkContext: SparkContext
  ): Unit = {
    val stagingHDFSDirectory: Option[List[FileStatus]] = readHDFSDirectory(stagingPath)
    val rawHDFSDirectory: Option[List[FileStatus]]     = readHDFSDirectory(rawPath)
    val masterHDFSrDirectory: Option[List[FileStatus]] = readHDFSDirectory(masterPath)
    val result = for {
      stagingHDFS <- stagingHDFSDirectory
      rawHDFS     <- rawHDFSDirectory
      masterHDFS  <- masterHDFSrDirectory
      resultStagingHDFS = processHDFSDirectory(stagingHDFS, stagingRawPattern)
      resultRawHDFS     = processHDFSDirectory(rawHDFS, stagingRawPattern)
      resultMasterHDFS  = processHDFSDirectory(masterHDFS, masterPattern)
    } yield validateDirectories(resultStagingHDFS, resultRawHDFS, resultMasterHDFS)
    if (result.getOrElse(false)) {
      AcmeLogger.logger.info("Success!! we have the same number of elements")
    } else {
      AcmeLogger.logger.error("An error occurred")
    }
  }

  private def transformPathToDirectory(path: String): (String, String) = {
    val pathArray       = path.split("/").map(_.trim)
    val directoryPath   = pathArray.take(pathArray.size - 1).mkString("/")
    val fileNamePattern = pathArray.takeRight(1).mkString
    (directoryPath, fileNamePattern)
  }

  private def executeProcess(stagingPath: String, rawPath: String, masterPath: String): Unit = {
    val (transformedStagingPath, filePattern) = transformPathToDirectory(stagingPath)
    val stagingDirectory: Option[File]        = readDirectory(stagingPath)
    val rawDirectory: Option[File]            = readDirectory(rawPath)
    val masterDirectory: Option[File]         = readDirectory(masterPath)
    val result = for {
      staging <- stagingDirectory
      raw     <- rawDirectory
      master  <- masterDirectory
      resultStaging = processDirectory(staging, stagingRawPattern)
      resultRaw     = processDirectory(raw, stagingRawPattern)
      resultMaster  = processDirectory(master, masterPattern)
    } yield validateDirectories(resultStaging, resultRaw, resultMaster)

    if (result.getOrElse(false)) {
      AcmeLogger.logger.info("Success!! we have the same number of elements")
    } else {
      AcmeLogger.logger.error("An error occurred")
    }
  }

  def showInformation(csvPath: String): Unit = {
    AcmeLogger.logger.info(s"Initializing Acme Program")
    val stageList: List[Stage] = AcmeModule.readCSV(csvPath)
    stageList.foreach(st => {
      AcmeLogger.logger.info("########################################")
      AcmeLogger.logger.info(s"Input Values : $st")
      AcmeModule.executeProcess(
        st.stagingPath,
        st.rawPath,
        st.masterPath
      )
      AcmeLogger.logger.info("########################################")
    })
  }
}
