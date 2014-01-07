package com.wajam.nrv.zookeeper.cluster

import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.service.ZookeeperService
import io.Source
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help

object ZookeeperClusterTool extends App {

  object Conf extends ScallopConf(args) {
    banner(
      """Usage: ./nrv-zookeeper/target/start [OPTION] SERVERS [FILE]
        |List SERVERS cluster configuration or diff/update SERVERS with FILE cluster configuration.
        |Examples: ./nrv-zookeeper/target/start -l 127.0.0.1:3000,127.0.0.1:3001/local
        |          ./nrv-zookeeper/target/start -d 127.0.0.1/local local.cluster
        |          ./nrv-zookeeper/target/start -u 127.0.0.1/local local.cluster
        | """.stripMargin)

    val ls = opt[Boolean]("ls", default=Some(false),
      descr = "print the current SERVERS cluster configuration")
    val all = opt[Boolean]("all", default=Some(false),
      descr = "print the entire SERVERS content including data and ephemeral nodes")
    val diff = opt[Boolean]("diff", default=Some(false),
      descr = "print the differences between SERVERS and FILE cluster configuration")
    val update = opt[Boolean]("update", default=Some(false),
      descr = "update SERVERS with the FILE cluster configuration")
    mutuallyExclusive(ls, all, diff, update)

    val servers = trailArg[String]("SERVERS", required = true,
      descr = "comma separated host:port pairs, each corresponding to a zk server e.g. " +
          "\"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002\". If the optional chroot suffix " +
          "is used the example would look like: \"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a\" " +
          "where the client would be rooted at \"/app/a\" and all paths would be relative to this root")
    val file = trailArg[String]("FILE", descr = "cluster configuration file", required = false)

    override protected def onError(e: Throwable) {
      e match {
        case _: Help =>
          builder.printHelp()
          sys.exit(0)
        case _ =>
          println("Error: %s".format(e.getMessage))
          println()
          builder.printHelp()
          sys.exit(1)
      }
    }
  }

  val zkClient = createZkClient(Conf.servers.apply())

  if (Conf.ls.apply()) {
    // List
    val zkConfig = readZookeeperConfig
    printLines(zkConfig)
  } else if (Conf.all.apply()) {
    // All
    val zkContent = readZookeeperContent
    printLines(zkContent)
  } else if (Conf.diff.apply()) {
    // Diff
    val zkConfig = readZookeeperConfig
    val fileLines = readFileConfig(Conf.file.apply())
    printDiff(new Diff(zkConfig.toMap, fileLines.toMap))
  } else if (Conf.update.apply()) {
    // Update
    val zkConfig = readZookeeperConfig
    val fileLines = readFileConfig(Conf.file.apply())
    updateDiff(new Diff(zkConfig.toMap, fileLines.toMap))
  }

  def createZkClient(servers: String): ZookeeperClient = {
    // Ensure root exists
    val pos = servers.indexOf("/")
    if (pos != -1) {
      val zkClient = new ZookeeperClient(servers.substring(0, pos))
      zkClient.ensureAllExists(servers.substring(pos), Array())
      zkClient.close()
    }
    new ZookeeperClient(servers, sessionTimeout = 30000)
  }

  def readZookeeperConfig: List[(String, String)] = {
    zkClient.ensureAllExists(ZookeeperService.rootPath, Array())

    // The following explicitly read the minimal cluster configuration structure ignoring any other elements.
    // Reading the configuration recursively would require ignore rules (e.g. ignore service 'data' nodes,
    // ephemeral nodes, and intermediate empty nodes).
    import ZookeeperClusterManager._
    var lines = List[(String, String)]()
    val services = zkClient.getChildren(ZookeeperService.rootPath)
    for (service <- services) {
      val servicePath = ZookeeperService.path(service)
      lines = (servicePath, zkClient.getString(servicePath)) :: lines
      val membersPath = ZookeeperService.membersPath(service)
      if (zkClient.exists(membersPath)) {
        val members = zkClient.getChildren(membersPath)
        for (member <- members.map(_.toLong)) {
          val memberPath = ZookeeperService.memberPath(service, member)
          lines = (memberPath, zkClient.getString(memberPath)) :: lines
          val votesPath = zkMemberVotesPath(service, member)
          if (zkClient.exists(votesPath)) {
            lines = (votesPath, "") :: lines
          }
          val replicasPath = zkMemberReplicasPath(service, member)
          if(zkClient.exists(replicasPath)) {
            lines = (replicasPath, zkClient.getString(replicasPath)) :: lines
          }
        }
      }
    }

    lines.sorted
  }

  def readFileConfig(filePath: String): List[(String, String)] = {
    val lines = Source.fromFile(filePath).getLines().map(_.trim).filter(line => !line.startsWith("#") && !line.isEmpty)
    lines.map(splitLine(_)).toList.sorted
  }

  def splitLine(line: String): (String, String) = {
    val pos = line.indexOf("=")
    if (pos > 0) (line.substring(0, pos), line.substring(pos + 1)) else (line, "")
  }

  def toString(path: String, value: String): String = {
    if (value.isEmpty) path else "%s=%s".format(path, value)
  }

  def printLines(lines: List[(String, String)]) {
    for (line <- lines) {
      println(toString(line._1, line._2))
    }
  }

  def printDiff(diff: Diff) {
    println("Remove")
    printLines(diff.removed)
    println()
    println("Add")
    printLines(diff.added)
    println()
    println("Update")
    printLines(diff.updated)
    println()
    println("Ignore")
    printLines(diff.ignored)
  }

  def updateDiff(diff: Diff) {
    println("Remove")
    for ((path, _) <- diff.removed.reverse) {
      println(path)
      zkClient.deleteRecursive(path)
    }

    println()
    println("Add")
    for ((path, value) <- diff.added) {
      println(toString(path, value))

      import com.wajam.nrv.zookeeper.ZookeeperClient._
      if (!zkClient.ensureAllExists(path, value)) {
        zkClient.set(path, value)
      }
    }

    println()
    println("Update")
    for ((path, value) <- diff.updated) {
      println(toString(path, value))

      import com.wajam.nrv.zookeeper.ZookeeperClient._
      zkClient.set(path, value)
    }

    println()
    println("Ignore")
    for ((path, value) <- diff.ignored) {
      println(toString(path, value))
    }
  }

  def readZookeeperContent: List[(String, String)] = {
    readZookeeperChildren("/", List[(String, String)]()).sorted
  }

  def readZookeeperChildren(path: String, lines: List[(String, String)]): List[(String, String)] = {
    val children = zkClient.getChildren(path)
    children.foldLeft((path, zkClient.getString(path)) :: lines)((l, c) => {
      val childPath = if (path.endsWith("/")) path + c else path + "/" + c
      readZookeeperChildren(childPath, l)
    })
  }

  class Diff(current: Map[String, String], target: Map[String, String]) {
    val removed = current.keys.toSeq.diff(target.keys.toSeq).map(key => (key, current(key))).toList.sorted
    val added = target.keys.toSeq.diff(current.keys.toSeq).map(key => (key, target(key))).toList.sorted
    val updated = intersectKeys.filter(key => !current(key).equals(target(key))).map(key => (key, target(key))).toList.sorted
    val ignored = intersectKeys.filter(key => current(key).equals(target(key))).map(key => (key, target(key))).toList.sorted

    private def intersectKeys = {
      current.keys.toSeq.intersect(target.keys.toSeq)
    }
  }
}
