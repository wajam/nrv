package com.wajam.nrv.zookeeper.consistency

import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import com.wajam.commons.Event
import com.wajam.nrv.consistency.ConsistencyPersistence
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Service, ServiceMember, NewMemberAddedEvent}
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient.{NodeChildrenChanged, NodeValueChanged}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import akka.actor.{Props, ActorSystem, Actor}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code
import ZookeeperConsistencyPersistence._

/**
 * Cluster configuration backed by Zookeeper.
 */
class ZookeeperConsistencyPersistence(zk: ZookeeperClient, service: Service)(implicit ec: ExecutionContext, as: ActorSystem) extends ConsistencyPersistence {

  private var mapping: ReplicasMapping = Map()
  private var mappingSequence: Int = 0

  private val serviceObserver: (Event) => Unit = {
    case NewMemberAddedEvent(member) =>
      updateReplicasMapping()
  }

  def start(): Unit = {
    // Synchronously load the mapping from Zookeeper
    synchronized {
      mapping = Await.result(mappingFuture, askTimeout.duration)._2
    }

    // Trigger an update when a new service member is added
    service.addObserver(serviceObserver)
  }

  def stop(): Unit = {
    service.removeObserver(serviceObserver)
  }

  def explicitReplicasMapping: Map[Long, List[Node]] = mapping

  def replicationLagSeconds(token: Long, node: Node): Option[Int] = ???

  def replicationLagSeconds_= (token: Long, node: Node, lag: Option[Int]) = ???

  def changeMasterServiceMember(token: Long, node: Node) = ???

  private def mappingFuture = mappingFetcher.ask(Fetch).mapTo[(Int, ReplicasMapping)]

  private def updateReplicasMapping(): Unit = {
    mappingFuture.onSuccess {
      case (newSequence: Int, newMapping: ReplicasMapping) =>
        synchronized {
          if (newSequence > mappingSequence) {
            mappingSequence = newSequence
            mapping = newMapping
          }
        }
    }
  }

  private implicit val askTimeout = Timeout(250)

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
          // Replicas path has been created: trigger an update
          if (zk.exists(replicasPath)) {
            updateReplicasMapping()
          }
          // Replicas path still doesn't exist: continue watching
          else {
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

  def parseNodeList(data: String): List[Node] = {
    data.split('|').map(Node.fromString).toList
  }
}
