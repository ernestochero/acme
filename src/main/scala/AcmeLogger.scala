import org.slf4j.{ Logger, LoggerFactory }
object AcmeLogger {
  implicit final val logger: Logger = LoggerFactory.getLogger("AcmeLogger")
}
