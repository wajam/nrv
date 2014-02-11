package com.wajam.nrv.zookeeper.consistency

import scala.language.postfixOps
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration._
import com.wajam.commons.{CurrentTime, Event}
import com.wajam.nrv.consistency.ConsistencyPersistence
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Service, ServiceMember, NewMemberAddedEvent}
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient.{NodeChildrenChanged, NodeValueChanged}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import akka.actor.{Props, ActorSystem, Actor}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.zookeeper.{CreateMode, KeeperException}
import org.apache.zookeeper.KeeperException.Code
import ZookeeperConsistencyPersistence._

/**
 * Cluster configuration backed by Zookeeper.
 *
 * @param updateThreshold the amount of seconds below which the replication lag should be persisted in Zookeeper right away
 * @param updateSpacing   the minimum amount of seconds between two updates of lag values over updateThreshold
 */
class ZookeeperConsistencyPersistence(zk: ZookeeperClient, service: Service, updateThreshold: Int, updateSpacing: Int)(implicit ec: ExecutionContext, as: ActorSystem) extends ConsistencyPersistence with CurrentTime {

  private var replicasMapping: ReplicasMapping = Map()
  private var mappingSequence: Int = 0

  private var replicationLagMap: ReplicationLagMap = Map()
  private var lastPersistedTs: Option[Long] = None

  private val serviceObserver: (Event) => Unit = {
    case NewMemberAddedEvent(member) =>
      updateReplicasMapping()

    case _ =>
  }

  def start(): Unit = {
    synchronized {
      // Synchronously load the replicas mapping from Zookeeper
      replicasMapping = Await.result(mappingFuture, askTimeout.duration)._2

      // Load the replication lags from Zookeeper
      replicationLagMap = fetchReplicationLagMap
    }

    // Trigger an update when a new service member is added
    service.addObserver(serviceObserver)
  }

  def stop(): Unit = {
    service.removeObserver(serviceObserver)
  }

  def explicitReplicasMapping: Map[Long, List[Node]] = replicasMapping

  override def replicationLagSeconds(token: Long, node: Node) = {
    replicationLagMap.get((token, node))
  }

  override def updateReplicationLagSeconds(token: Long, node: Node, lag: Int): Unit = {
    import ZookeeperClient.int2bytes

    def persistLag(): Unit = {
      synchronized {
        replicationLagMap += (token, node) -> lag
        lastPersistedTs = Some(currentTime)
      }

      val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, node)

      // Persist value in Zookeeper
      try {
        zk.set(path, lag)
      } catch {
        // Node doesn't exist, create it
        case e: KeeperException if e.code == Code.NONODE =>
          zk.create(path, lag, CreateMode.PERSISTENT)
      }
    }

    replicationLagSeconds(token, node) match {
      case Some(currentLag) if currentLag <= updateThreshold || lag <= updateThreshold =>
        // Lag has significantly changed, immediately persist its value
        persistLag()

      case Some(currentLag) =>
        // Lag has not significantly changed: persist according to rate limit
        lastPersistedTs match {
          case Some(ts) if currentTime - ts >= updateSpacing =>
            // More than updateSpacing seconds elapsed since last persisted update: persist
            persistLag()

          case None =>
            // No clue about last persisted update time: persist
            persistLag()

          case _ =>
        }

      case None =>
        // No previously persisted value: persist
        persistLag()
    }
  }

  // Fetch replication lag for each slave, based on the current replicasMapping
  private def fetchReplicationLagMap: ReplicationLagMap = {
    replicasMapping.flatMap { case (token, nodes) =>
      nodes.flatMap { node =>
        val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, node)

        try {
          val value = zk.getInt(path)
          Some((token, node) -> value)
        } catch {
          case e: KeeperException if e.code == Code.NONODE =>
            None

          case e: Throwable =>
            throw e
        }
      }
    }.toMap
  }

  def changeMasterServiceMember(token: Long, node: Node) = Unit

  private def mappingFuture = mappingFetcher.ask(Fetch).mapTo[(Int, ReplicasMapping)]

  private def updateReplicasMapping(): Unit = {
    mappingFuture.onSuccess {
      case (newSequence: Int, newMapping: ReplicasMapping) =>
        synchronized {
          if (newSequence > mappingSequence) {
            mappingSequence = newSequence
            replicasMapping = newMapping
          }
        }
    }
  }

  private implicit val askTimeout = Timeout(1 second)

  private val mappingFetcher = as.actorOf(Props(
    new Actor {

      // Callbacks must be instantiated once then cached so that ZookeeperClient doesn't create watch duplicates
      private var newMemberChildCallbacks: Map[String, NodeChildrenChanged => Unit] = Map()
      private val replicasChangeCallback = (e: NodeValueChanged) => updateReplicasMapping()

      private var sequence = 0

      def receive = {
        case Fetch =>
          val mapping = fetchReplicasMapping
          sequence = sequence + 1

          sender ! (sequence, mapping)
      }

      // Fetches the entire (token -> replicas) mapping for the service.
      private def fetchReplicasMapping: ReplicasMapping = {
        service.members.map { member =>
          // Get the node value from Zookeeper and watch for changes
          val replicas = fetchMemberReplicas(member)

          member.token -> replicas
        }.toMap
      }

      // Fetch list of replicas for the token associated to the given service member, if it exists in Zk.
      // Otherwise, create a watcher to be notified when the replicas are set.
      private def fetchMemberReplicas(member: ServiceMember): List[Node] = {
        val replicasPath = ZookeeperClusterManager.zkMemberReplicasPath(service.name, member.token)

        try {
          val replicaString = zk.getString(replicasPath, Some(replicasChangeCallback))
          parseNodeList(replicaString)
        } catch {
          // Replicas are not set yet: watch them
          case e: KeeperException if e.code == Code.NONODE =>
            watchMemberReplicas(member, replicasPath)
            Nil

          case e: Throwable =>
            throw e
        }
      }

      // Watch for the creation of a /replicas node for the token associated to the given service member.
      // Trigger an update when the replicas are set.
      // Usually useful when a new service member is added and its replicas are not immediately set.
      private def watchMemberReplicas(member: ServiceMember, replicasPath: String): Unit = {
        val memberPath = ZookeeperClusterManager.zkMemberPath(service.name, member.token)

        def newMemberChildCallback(e: NodeChildrenChanged) = {
          if (zk.exists(replicasPath)) {
            // Replicas path has been created: trigger an update
            updateReplicasMapping()
          } else {
            // Replicas path still doesn't exist: continue watching
            watchMemberReplicas(member, replicasPath)
          }
        }

        val callback = newMemberChildCallbacks.get(memberPath).getOrElse {
          val newCallback = newMemberChildCallback _
          newMemberChildCallbacks += (memberPath -> newCallback)
          newCallback
        }

        zk.getChildren(memberPath, Some(callback))
      }
    }
  ))

  case object Fetch

}

object ZookeeperConsistencyPersistence {

  type ReplicasMapping = Map[Long, List[Node]]
  type ReplicationLagMap = Map[(Long, Node), Int]

  def parseNodeList(data: String): List[Node] = {
    data.split('|').map(Node.fromString).toList
  }
}
