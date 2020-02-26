import org.log4s.getLogger

object AcmeLogger {
  implicit final val logger: org.log4s.Logger = getLogger("AcmeLogger")
}
