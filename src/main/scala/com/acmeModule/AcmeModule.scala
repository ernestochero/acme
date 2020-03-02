package com.acmeModule

import java.io.{ File, FileFilter }

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import com.models.Stage
import com.utils.Pattern._
import org.apache.commons.io.filefilter.WildcardFileFilter

import scala.util.matching.Regex

object AcmeModule {
  private def readHDFSDirectory(
    strPath: String
  )(implicit sparkContext: SparkContext): Option[FileSystem] = {
    val directoryPath = new Path(strPath)
    val fileSystem    = FileSystem.get(sparkContext.hadoopConfiguration)
    if (fileSystem.isDirectory(directoryPath))
      Some(fileSystem)
    else None
  }

  implicit lazy val wildcardFileFilter: (String => FileFilter) = (fileNamePattern: String) =>
    new WildcardFileFilter(fileNamePattern)

  private def processHDFSDirectory(filePath: String,
                                   fileSystem: FileSystem,
                                   fileDatePattern: Regex): List[Option[String]] = {
    // https://stackoverflow.com/questions/24647992/wildcard-in-hadoops-filesystem-listing-api-calls
    val fileStatus = fileSystem.globStatus(new Path(filePath))
    fileStatus.map(_.getPath.getName).map(name => fileDatePattern findFirstIn name).toList
  }

  private def readDirectory(path: String): Option[File] = {
    val file = new File(path)
    if (file.isDirectory) Some(file)
    else None
  }

  private def processDirectory(stage: File, fileDatePattern: Regex, fileNamePattern: String = "")(
    implicit wildCardFileFilter: String => FileFilter
  ): List[Option[String]] = {
    val filteredFileNames =
      if (fileNamePattern.isEmpty) { stage.listFiles().map(_.getName) } else {
        stage.listFiles(wildCardFileFilter(fileNamePattern)).map(_.getName)
      }
    filteredFileNames.map(name => fileDatePattern findFirstIn name).toList
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
    val (transformedStagingPath, _)              = transformPathToDirectory(stagingPath)
    val stagingHDFSDirectory: Option[FileSystem] = readHDFSDirectory(transformedStagingPath)
    val rawHDFSDirectory: Option[FileSystem]     = readHDFSDirectory(rawPath)
    val masterHDFSrDirectory: Option[FileSystem] = readHDFSDirectory(masterPath)
    val result = for {
      stagingHDFS <- stagingHDFSDirectory
      rawHDFS     <- rawHDFSDirectory
      masterHDFS  <- masterHDFSrDirectory
      resultStagingHDFS = processHDFSDirectory(stagingPath, stagingHDFS, stagingRawDatePattern)
      resultRawHDFS     = processHDFSDirectory(rawPath, rawHDFS, stagingRawDatePattern)
      resultMasterHDFS  = processHDFSDirectory(masterPath, masterHDFS, masterDatePattern)
    } yield validateDirectories(resultStagingHDFS, resultRawHDFS, resultMasterHDFS)
    if (result.getOrElse(false)) {
      println("Success!! we have the same number of elements")
    } else {
      println("An error occurred")
    }
  }

  private def transformPathToDirectory(path: String): (String, String) = {
    val pathArray       = path.split("/").map(_.trim)
    val directoryPath   = pathArray.take(pathArray.size - 1).mkString("/")
    val fileNamePattern = pathArray.takeRight(1).mkString
    (directoryPath, fileNamePattern)
  }

  private def executeProcess(stagingPath: String, rawPath: String, masterPath: String): Unit = {
    val (transformedStagingPath, stagingFileNamePattern) = transformPathToDirectory(stagingPath)
    val stagingDirectory: Option[File]                   = readDirectory(transformedStagingPath)
    val rawDirectory: Option[File]                       = readDirectory(rawPath)
    val masterDirectory: Option[File]                    = readDirectory(masterPath)
    val result = for {
      staging <- stagingDirectory
      raw     <- rawDirectory
      master  <- masterDirectory
      resultStaging = processDirectory(staging, stagingRawDatePattern, stagingFileNamePattern)
      resultRaw     = processDirectory(raw, stagingRawDatePattern)
      resultMaster  = processDirectory(master, masterDatePattern)
      _             = println(resultStaging)
      _             = println(resultRaw)
      _             = println(resultMaster)
    } yield validateDirectories(resultStaging, resultRaw, resultMaster)

    if (result.getOrElse(false)) {
      println("Success!! we have the same number of elements")
    } else {
      println("An error occurred")
    }
  }

  def showInformationHDFS(csvPath: String): Unit = {
    import com.sparkConfiguration.SparkConfiguration.sc
    println(s"Initializing HDFS Acme Program")
    val stageList: List[Stage] = AcmeModule.readCSV(csvPath)
    stageList.foreach(st => {
      println("########################################")
      println(s"Input Values : $st")
      AcmeModule.executeProcessHDFS(
        st.stagingPath,
        st.rawPath,
        st.masterPath
      )
      println("########################################")
    })
  }

  def showInformation(csvPath: String): Unit = {
    println(s"Initializing Acme Program")
    val stageList: List[Stage] = AcmeModule.readCSV(csvPath)
    stageList.foreach(st => {
      println("########################################")
      println(s"Input Values : $st")
      AcmeModule.executeProcess(
        st.stagingPath,
        st.rawPath,
        st.masterPath
      )
      println("########################################")
    })
  }
}
