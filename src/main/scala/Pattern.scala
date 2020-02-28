import scala.util.matching.Regex

object Pattern {
  val stagingRawPattern: Regex = "\\d{8}".r
  val masterPattern: Regex     = "(\\d{1,4}([.\\-])\\d{1,2}([.\\-])\\d{1,4})".r
}
