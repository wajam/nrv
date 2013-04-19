package com.wajam.nrv.scribe

import scribe.thrift.{ResultCode, LogEntry}
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import java.net.Socket
import org.apache.thrift.protocol.TBinaryProtocol
import scribe.thrift.Scribe.Client
import java.util.Collections
import org.apache.thrift.TException
import scala.actors.Actor
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * Asynchronous scribe client
 */
class ScribeClient(scribeCategory: String, scribeHost: String, scribePort: Int) extends Actor with Logging with Instrumented {
  private val logCall = metrics.meter("log-call", "log-call", scribeCategory)
  private val logError = metrics.meter("log-error", "log-error", scribeCategory)
  private val logDrop = metrics.meter("log-drop", "log-drop", scribeCategory)
  private val resultOk = metrics.meter("result-ok", "result-ok", scribeCategory)
  private val resultTryLater = metrics.meter("result-trylater", "result-trylater", scribeCategory)
  private val queueSize = metrics.gauge("queue-size", scribeCategory) {
    mailboxSize
  }

  private var scribeClient: Option[Client] = None

  /**
   * Write the specified message asynchronously to Scribe
   */
  def log(message: String) {
    this ! new LogEntry(scribeCategory, message)
  }

  def act() {
    while (true) {
      receive {
        case entry: LogEntry => log(entry)
      }
    }
  }

  private def log(entry: LogEntry) {
    if (!isConnected) {
      connect()
    }

    scribeClient match {
      case Some(client) =>
        try {
          logCall.mark()
          val result = client.Log(Collections.singletonList(entry))
          result match {
            case ResultCode.OK => resultOk.mark()
            case ResultCode.TRY_LATER => resultTryLater.mark()
          }
        } catch {
          case e: TException =>
            logError.mark()
            disconnect()
        }
      case None =>
        info("Not connected to scribe! Dropping log entry.")
        logDrop.mark()
    }
  }

  private def isConnected: Boolean = {
    scribeClient.isDefined
  }

  private def connect() {
    var socket: TSocket = null
    scribeClient = try {
      socket = new TSocket(new Socket(scribeHost, scribePort))
      val transport = new TFramedTransport(socket)
      val protocol = new TBinaryProtocol(transport, false, false)

      Some(new Client(protocol, protocol))
    } catch {
      case e: Exception =>
        if (socket != null) {
          socket.close()
        }
        disconnect()
        info("Unable to connect to scribe {}:{} {}", scribeHost, scribePort, e.getMessage)

        None
    }
  }

  private def disconnect() {
    val clientToDisconect = scribeClient
    scribeClient = None

    for (client <- clientToDisconect) {
      try {
        client.shutdown()
      } catch {
        case e: TException =>
          info("Error disconnecting from scribe  {}:{} {}", scribeHost, scribePort, e.getMessage)
      }
    }
  }
}
