package com.acmeModule

import java.io.{ File, FileFilter }

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import com.models.Stage
import com.utils.Pattern._
import org.apache.commons.io.filefilter.WildcardFileFilter
import scala.util.{ Either, Left, Right }
import scala.util.matching.Regex

object AcmeModule {
  private def readHDFSDirectory(
    strPath: String
  )(implicit sparkContext: SparkContext): Either[Exception, FileSystem] = {
    val directoryPath          = new Path(strPath)
    val fileSystem: FileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    if (fileSystem.isDirectory(directoryPath))
      Right(fileSystem)
    else Left(new Exception(s"$strPath is not a directory"))
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

  private def parseResult(booleanResult: Boolean): String =
    if (booleanResult) "Succeeded"
    else "Failed"

  private def showResult(stageNameA: String,
                         stageA: List[Option[String]],
                         stageNameB: String,
                         stageB: List[Option[String]]): Unit = {
    val stageASize   = stageA.count(_.isDefined)
    val stageBSize   = stageB.count(_.isDefined)
    val resultBySize = stageASize == stageBSize
    val result       = parseResult(resultBySize)
    println("-" * 50)
    println(s"$stageA => [$stageASize] vs $stageB => [$stageBSize] = ($result)")
    println("-" * 50)
  }

  private def validateDirectoriesResult(stagingDirectory: List[Option[String]],
                                        rawDirectory: List[Option[String]],
                                        masterDirectory: List[Option[String]]): Unit = {
    // Staging vs Raw
    showResult(stageNameA = "Staging",
               stageA = stagingDirectory,
               stageNameB = "Raw",
               stageB = rawDirectory)

    // Raw vs Master
    showResult(stageNameA = "Raw",
               stageA = rawDirectory,
               stageNameB = "Master",
               stageB = masterDirectory)
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

  def executeProcessHDFS(stagingPath: String, rawPath: String, masterPath: String)(
    implicit sparkContext: SparkContext
  ): Unit = {
    val (transformedStagingPath, _) = transformPathToDirectory(stagingPath)
    val stagingHDFSDirectory: Either[Exception, FileSystem] = readHDFSDirectory(
      transformedStagingPath
    )
    val rawHDFSDirectory: Either[Exception, FileSystem]     = readHDFSDirectory(rawPath)
    val masterHDFSrDirectory: Either[Exception, FileSystem] = readHDFSDirectory(masterPath)
    for {
      stagingHDFS <- stagingHDFSDirectory.right
      rawHDFS     <- rawHDFSDirectory.right
      masterHDFS  <- masterHDFSrDirectory.right
    } yield {
      val resultStagingHDFS = processHDFSDirectory(stagingPath, stagingHDFS, stagingRawDatePattern)
      val resultRawHDFS     = processHDFSDirectory(rawPath, rawHDFS, stagingRawDatePattern)
      val resultMasterHDFS  = processHDFSDirectory(masterPath, masterHDFS, masterDatePattern)
      validateDirectoriesResult(resultStagingHDFS, resultRawHDFS, resultMasterHDFS)
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
      println("#" * 50)
      println(s"Input Values : $st")
      AcmeModule.executeProcessHDFS(
        st.stagingPath,
        st.rawPath,
        st.masterPath
      )
      println("#" * 50)
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
