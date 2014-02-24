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
import com.wajam.nrv.zookeeper.ZookeeperClient.{NodeValueChanged, NodeStatusChanged}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import akka.actor.{Props, ActorSystem, Actor}
import akka.agent.Agent
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
class ZookeeperConsistencyPersistence(zk: ZookeeperClient, service: Service, updateThreshold: Int, updateSpacing: Int, waitTimeoutSeconds: Int = 10)(implicit ec: ExecutionContext, as: ActorSystem) extends ConsistencyPersistence with CurrentTime {
  import ReplicationLagPersistence._

  private var replicasMapping: ReplicasMapping = Map()
  private var mappingSequence: Int = 0

  private val serviceObserver: (Event) => Unit = {
    case NewMemberAddedEvent(member) => updateReplicasMapping()
    case _ =>
  }

  val waitTimeout = waitTimeoutSeconds.seconds
  implicit val askTimeout = Timeout(waitTimeout)

  def start(): Unit = {
    // Synchronously load the replicas mapping from Zookeeper
    replicasMapping = Await.result(mappingFuture, waitTimeout)._2

    // Synchronously load the replication lags from Zookeeper
    lagMapAgent.send(fetchReplicationLagMap _)
    Await.result(lagMapAgent.future, waitTimeout)

    // Trigger an update when a new service member is added
    service.addObserver(serviceObserver)
  }

  def stop(): Unit = {
    service.removeObserver(serviceObserver)
  }

  def explicitReplicasMapping: Map[Long, List[Node]] = replicasMapping

  // The map of replication lags is handled by an Agent.
  // The ReplicationLagPersistence object defines all the update methods that can be passed to this agent.
  private[this] object ReplicationLagPersistence {

    val lagMapAgent = Agent[ReplicationLagMap](Map())

    private var lastPersistedTs: Option[Long] = None

    private var lagValueChangedCallbacks: Map[String, (NodeValueChanged) => Unit] = Map()
    private var lagStatusChangedCallbacks: Map[String, (NodeStatusChanged) => Unit] = Map()

    // Fetch replication lag for each slave, based on the current replicasMapping, and return the map
    def fetchReplicationLagMap(currentLagMap: ReplicationLagMap): ReplicationLagMap = {
      replicasMapping.flatMap { case (token, slaves) =>
        slaves.flatMap { slave =>
          fetchReplicationLagValue(token, slave).map((token, slave) -> _)
        }
      }.toMap
    }

    // Fetch replication lag for a given slave, and return the updated
    def fetchReplicationLag(token: Long, slave: Node)(currentLagMap: ReplicationLagMap): ReplicationLagMap = {
      fetchReplicationLagValue(token, slave)  match {
        case Some(value) =>
          currentLagMap + ((token, slave) -> value)
        case None =>
          currentLagMap - ((token, slave))
      }
    }

    // Fetch replication lag for a given slave, watch for changes, and for node creation if doesn't exist
    private def fetchReplicationLagValue(token: Long, slave: Node): Option[Int] = {
      val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, slave)

      // Register a callback triggered when the Zookeeper node will be either created or deleted
      val statusCallback = lagStatusChangedCallbacks.get(path).getOrElse {
        val newCallback = (e: NodeStatusChanged) => lagMapAgent.send(fetchReplicationLag(token, slave) _)
        lagStatusChangedCallbacks += (path -> newCallback)
        newCallback
      }
      zk.exists(path, Some(statusCallback)) match {
        case true => {
          val valueCallback = lagValueChangedCallbacks.get(path).getOrElse {
            val newCallback = (e: NodeValueChanged) => lagMapAgent.send(fetchReplicationLag(token, slave) _)
            lagValueChangedCallbacks += (path -> newCallback)
            newCallback
          }

          try {
            Some(zk.getInt(path, Some(valueCallback)))
          } catch {
            case e: KeeperException if e.code == Code.NONODE =>
              // Node has been deleted between exists() and getInt() calls: will be handled by the status callback
              None
            case e: Throwable => throw e
          }
        }
        case false => None
      }
    }

    /**
     * Persist a new replication lag value, according to the rate limiting strategy:
     * - If either the persisted value or the new value is below the updateThreshold, persist now
     * - If more than updateSpacing elapsed since the last persisted value, persist now
     * - If there was no persisted value, persist now
     * - Otherwise, don't persist nor cache the new value
     *
     * This logic has to be done in the Agent to ensure that currentLag is not accessed concurrently.
     */
    def persistReplicationLag(token: Long, node: Node, lag: Int)(currentLagMap: ReplicationLagMap): ReplicationLagMap = {
      import ZookeeperClient.int2bytes

      def persistLag(): ReplicationLagMap = {
        lastPersistedTs = Some(currentTime)

        val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, node)

        // Persist value in Zookeeper
        try {
          zk.set(path, lag)
        } catch {
          // Node doesn't exist, create it
          case e: KeeperException if e.code == Code.NONODE => zk.ensureAllExists(path, lag, CreateMode.PERSISTENT)
          case e: Throwable => throw e
        }

        currentLagMap + ((token, node) -> lag)
      }

      replicationLagSeconds(token, node) match {
        // Lag has significantly changed, immediately persist its value
        case Some(currentLag) if currentLag <= updateThreshold || lag <= updateThreshold => persistLag()
        case Some(currentLag) => {
          // Lag has not significantly changed: persist according to rate limit
          lastPersistedTs match {
            // More than updateSpacing seconds elapsed since last persisted update: persist
            case Some(ts) if currentTime - ts >= updateSpacing => persistLag()
            // No clue about last persisted update time: persist
            case None => persistLag()
            case _ => currentLagMap
          }
        }
        // No previously persisted value: persist
        case None => persistLag()
      }
    }
  }

  override def replicationLagSeconds(token: Long, node: Node) = {
    lagMapAgent().get((token, node))
  }

  override def updateReplicationLagSeconds(token: Long, node: Node, lag: Int): Unit = {
    lagMapAgent.send(persistReplicationLag(token, node, lag) _)
  }

  def changeMasterServiceMember(token: Long, node: Node) = Unit

  private def mappingFuture = mappingFetcher.ask(Fetch).mapTo[(Int, ReplicasMapping)]

  private def updateReplicasMapping(): Unit = {
    mappingFuture.onSuccess {
      case (newSequence: Int, newMapping: ReplicasMapping) => {
        synchronized {
          if (newSequence > mappingSequence) {
            mappingSequence = newSequence
            replicasMapping = newMapping
          }
        }
        // As new slaves might have been added, reload the entire lag map
        lagMapAgent.send(fetchReplicationLagMap _)
      }
    }
  }

  private val mappingFetcher = as.actorOf(Props(
    new Actor {

      // Callbacks must be instantiated once then cached so that ZookeeperClient doesn't create watch duplicates
      private var replicasCreatedCallbacks: Map[String, NodeStatusChanged => Unit] = Map()
      private val replicasChangedCallback = (e: NodeValueChanged) => updateReplicasMapping()

      private var sequence = 0

      def receive = {
        case Fetch => {
          val mapping = fetchReplicasMapping
          sequence = sequence + 1

          sender ! (sequence, mapping)
        }
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
          val replicaString = zk.getString(replicasPath, Some(replicasChangedCallback))
          parseNodeList(replicaString)
        } catch {
          case e: KeeperException if e.code == Code.NONODE => {
            // Watch for the creation of a /replicas node and trigger an update when the replicas are set.
            // Usually useful when a new service member is added and its replicas are not immediately set.
            val callback = replicasCreatedCallbacks.get(replicasPath).getOrElse {
              val newCallback = (e: NodeStatusChanged) => updateReplicasMapping()
              replicasCreatedCallbacks += (replicasPath -> newCallback)
              newCallback
            }
            zk.exists(replicasPath, Some(callback))
            Nil
          }
          case e: Throwable => throw e
        }
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
