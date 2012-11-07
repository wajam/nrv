package com.wajam.nrv.utils

import java.net.{Inet4Address, NetworkInterface, InetAddress}

/**
 * Internet Protocol utilities
 */
object InetUtils {

  lazy val firstInetAddress: Option[InetAddress] = {
    import scala.collection.JavaConversions._
    val nic = NetworkInterface.getNetworkInterfaces.find(nic => !nic.isLoopback && nic.isUp)
    nic match {
      case Some(n) => n.getInetAddresses.find(_.isInstanceOf[Inet4Address])
      case _ => None
    }
  }

}
