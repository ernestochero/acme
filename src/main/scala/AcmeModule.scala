import java.io.File

import AcmeModule.Service
import zio.ZIO

import scala.util.matching.Regex
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
trait AcmeModule extends Serializable {
  val acmeModule: Service[Any]
}

object Pattern {
  val stagingRawPattern: Regex = "\\d{8}".r
  val masterPattern: Regex     = "(\\d{1,4}([.\\-])\\d{1,2}([.\\-])\\d{1,4})".r
}

object SparkConfiguration {
  val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext
}

object AcmeModule {
  import Pattern._
  import SparkConfiguration._
  case class Acme() {

    private def readHDFSDirectory(strPath: String): Option[List[FileStatus]] = {
      val directoryPath = new Path(strPath)
      val fileSystem    = FileSystem.get(sc.hadoopConfiguration)
      if (fileSystem.isDirectory(directoryPath))
        Some(fileSystem.listStatus(directoryPath).toList)
      else None
    }

    private def processHDFSDirectory(stage: List[FileStatus],
                                     pattern: Regex): List[Option[String]] =
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

    def createStageObject(csvLine: String): Stage = {
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

    def executeProcessHDFS(stagingPath: String, rawPath: String, masterPath: String): Unit = {
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

    def transformPathToDirectory(path: String): (String, String) = {
      val pathArray       = path.split("/").map(_.trim)
      val directoryPath   = pathArray.take(pathArray.size - 1).mkString("/")
      val fileNamePattern = pathArray.takeRight(1).mkString
      (directoryPath, fileNamePattern)
    }

    def executeProcess(stagingPath: String, rawPath: String, masterPath: String): Unit = {
      val (transformedStagingPath, filePattern) = transformPathToDirectory(stagingPath)
      val stagingDirectory: Option[File]        = readDirectory(transformedStagingPath, filePattern)
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
  }

  trait Service[R] extends Serializable {
    def acmeService: ZIO[R, Throwable, Acme]
  }
  trait Live extends AcmeModule {
    override val acmeModule: Service[Any] = new Service[Any] {
      override def acmeService: ZIO[Any, Throwable, Acme] =
        ZIO.succeed(Acme())
    }
  }

  object factory extends Service[AcmeModule] {
    override def acmeService: ZIO[AcmeModule, Throwable, Acme] =
      ZIO.accessM[AcmeModule](_.acmeModule.acmeService)
  }

}
