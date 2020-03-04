package com.main
import com.acmeModule.AcmeModule._
object Server extends App {
  override def main(args: Array[String]): Unit = {
    val result = readCSV("C:\\Users\\Indra\\Documents\\ingestion\\path.csv")
    result.foreach(println)
  }
}
