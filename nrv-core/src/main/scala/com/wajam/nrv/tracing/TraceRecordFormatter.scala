package com.wajam.nrv.tracing

import java.text.SimpleDateFormat

/**
 * Format trace record in a tab delimited string.
 * <p>
 * <p>
 * Here is the tab delimited fields sequence (top to bottom) and their presence according the recorded annotation
 * ([ ]=empty, X=yes, !=optional):
 * <p>
 * <pre>
 *               ClientSend  ClientRecv  ServerSend  ServerRecv  Message   ClientAddress ServerAddress
 * timestamp (1) X           X           X           X           X           X           X
 * duration                                                      !
 * type          X           X           X           X           X           X           X
 * traceid       X           X           X           X           X           X           X
 * spanid        X           X           X           X           X           X           X
 * parentspanid  !           !           !           !           !           !           !
 * service       X                                   X
 * protocol      X                                   X
 * method        !                                   !
 * path          X                                   X
 * hostname                                                                  !           !
 * ipaddress                                                                 X           X
 * port                                                                      X           X
 * responsecode              !           !
 * msgcontent                                                    X
 * msgsource                                                     !
 *
 * (1) ISO8601 format YYYY-MM-DDTHH:MM:SS.SSSZ
 * </pre>
 */
object TraceRecordFormatter {

  val typeMap = Map[Class[_], String](
    classOf[Annotation.ClientSend] -> "ClientSend", classOf[Annotation.ClientRecv] -> "ClientRecv",
    classOf[Annotation.ServerSend] -> "ServerSend", classOf[Annotation.ServerRecv] -> "ServerRecv",
    classOf[Annotation.ClientAddress] -> "ClientAddress", classOf[Annotation.ServerAddress] -> "ServerAddress",
    classOf[Annotation.Message] -> "Message"
  )

  /**
   * Returns a tab delimited string representation of the specified record.
   */
  def record2TabSeparatedString(record: Record): String = record2Traversable(record).mkString("\t")

  /**
   * Returns a traversable sequence of record field values.
   */
  def record2Traversable(record: Record, msgEscaper: (String) => String = _.replace("\t", " ")): Traversable[String] = new Traversable[String] {

    def foreach[U](f: (String) => U) {

      // Record
      //  timestamp (ISO8601 YYYY-MM-DDTHH:MM:SS.SSSZ)
      f(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").format(record.timestamp))
      //  duration
      f(record.duration.getOrElse("").toString)
      //  type
      f(typeMap(record.annotation.getClass))

      // TraceContext
      //  traceid
      f(record.context.traceId)
      //  spanid
      f(record.context.spanId)
      //  parentspanid
      f(record.context.parentSpanId.getOrElse(""))

      // ClientSend / ServerRecv
      val (service, protocol, method, path) = annotation2RpcNameComponents(record.annotation)
      //  service
      f(service.getOrElse(""))
      //  protocol
      f(protocol.getOrElse(""))
      //  method
      f(method.getOrElse(""))
      //  path
      f(path.getOrElse(""))

      // ClientAddress / ServerAddress
      val (hostname, ipaddress, port) = annotation2AddressComponents(record.annotation)
      //  hostname
      f(hostname.getOrElse(""))
      //  ipaddress
      f(ipaddress.getOrElse(""))
      //  port
      f(port.getOrElse("").toString)

      // ClientRecv / ServerSend
      val code = annotation2ResponseCode(record.annotation)
      //  responsecode
      f(code.getOrElse("").toString)

      // Message
      val (content, source) = annotation2MessageComponents(record.annotation, msgEscaper)
      //  msgcontent
      f(content)
      //  msgsource
      f(source.getOrElse(""))
    }

    /**
     * Returns a tuple with the different RpcName components (service, protocol, method, path) if the annotation is ClientSend or ServerRecv
     */
    def annotation2RpcNameComponents(annotation: Annotation): (Option[String], Option[String], Option[String], Option[String]) = {
      annotation match {
        case cs: Annotation.ClientSend =>
          val name = cs.name
          (Option(name.service), Option(name.protocol), Option(name.method), Option(name.path))
        case sr: Annotation.ServerRecv =>
          val name = sr.name
          (Option(name.service), Option(name.protocol), Option(name.method), Option(name.path))
        case _ =>
          (None, None, None, None)
      }
    }

    /**
     * Returns a tuple with the different address components (hostname, ipaddress, port) if the annotation is
     * ClientAddress or ServerAddress
     */
    private def annotation2AddressComponents(annotation: Annotation): (Option[String], Option[String], Option[Int]) = {
      annotation match {
        case ca: Annotation.ClientAddress =>
          val addr = ca.addr
          (Option(addr.getHostName), Option(addr.getAddress.getHostAddress), Option(addr.getPort))
        case sa: Annotation.ServerAddress =>
          val addr = sa.addr
          (Option(addr.getHostName), Option(addr.getAddress.getHostAddress), Option(addr.getPort))
        case _ =>
          (None, None, None)
      }
    }

    /**
     * Returns the reponse code if the annotation is ClientRecv or ServerSend
     */
    private def annotation2ResponseCode(annotation: Annotation): Option[Int] = {
      annotation match {
        case cr: Annotation.ClientRecv =>
          cr.code
        case ss: Annotation.ServerSend =>
          ss.code
        case _ =>
          None
      }
    }

    /**
     * Returns a tuple with the different message components (content, source) if the annotation is Message
     */
    private def annotation2MessageComponents(annotation: Annotation, msgEscaper: (String) => String): (String, Option[String]) = {
      annotation match {
        case msg: Annotation.Message  =>
          (msgEscaper(msg.content), msg.source)
        case _ =>
          ("", None)
      }
    }
  }
}
