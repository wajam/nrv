package com.wajam.nrv.zookeeper.consistency

import scala.language.postfixOps
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration._
import com.wajam.commons.{Logging, CurrentTime, Event}
import com.wajam.nrv.consistency.ConsistencyPersistence
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Service, ServiceMember, NewMemberAddedEvent}
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient.{NodeEvent, NodeValueChanged, NodeStatusChanged}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import akka.actor.ActorSystem
import akka.agent.Agent
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
class ZookeeperConsistencyPersistence(zk: ZookeeperClient, service: Service, updateThreshold: Int, updateSpacing: Int, waitTimeoutSeconds: Int = 10)(implicit ec: ExecutionContext, as: ActorSystem) extends ConsistencyPersistence with CurrentTime with Logging {
  import ReplicationLagPersistence._
  import ReplicasMappingPersistence._

  private val serviceObserver: (Event) => Unit = {
    case NewMemberAddedEvent(member) => updateMemberReplicasThenLags(member)
    case _ =>
  }

  val waitTimeout = waitTimeoutSeconds.seconds
  implicit val askTimeout = Timeout(waitTimeout)

  def start(): Unit = {
    // Synchronously load the replicas mapping from Zookeeper
    replicasMappingAgent.send(updateReplicasMapping _)
    Await.result(replicasMappingAgent.future, waitTimeout)

    // Synchronously load the replication lags from Zookeeper
    lagMapAgent.send(updateReplicationLagMap _)
    Await.result(lagMapAgent.future, waitTimeout)

    // Trigger an update when a new service member is added
    service.addObserver(serviceObserver)
  }

  def stop(): Unit = {
    service.removeObserver(serviceObserver)
  }

  def explicitReplicasMapping: Map[Long, List[Node]] = replicasMappingAgent().replicas

  def replicationLagSeconds(token: Long, node: Node) = {
    lagMapAgent().get((token, node))
  }

  def updateReplicationLagSeconds(token: Long, node: Node, lag: Int): Unit = {
    lagMapAgent.send(persistReplicationLag(token, node, lag) _)
  }

  def changeMasterServiceMember(token: Long, node: Node): Unit = {
    import ZookeeperClient.string2bytes

    explicitReplicasMapping.get(token) match {
      case Some(nodes) if nodes.contains(node) => {
        val path = ZookeeperClusterManager.zkMemberPath(service.name, token)
        val serviceMember = new ServiceMember(token, new Node(node.hostname, Map("nrv" -> node.ports("nrv"))))

        zk.set(path, serviceMember.toString)
      }
      case Some(_) => throw new IllegalArgumentException("Node is not a slave on this shard")
      case None => throw new IllegalArgumentException("Token not found")
    }
  }

  // The map of replication lags is handled by an Agent.
  // The ReplicationLagPersistence object defines all the update methods that can be passed to this agent.
  private[this] object ReplicationLagPersistence {

    val lagMapAgent = Agent[ReplicationLagMap](Map())

    private var lastPersistedTs: Option[Long] = None

    private var lagValueChangedCallbacks: Map[String, (NodeValueChanged) => Unit] = Map()
    private var lagStatusChangedCallbacks: Map[String, (NodeStatusChanged) => Unit] = Map()

    // Fetch replication lag for all slaves of all shards, based on the current replicasMapping
    def updateReplicationLagMap(currentLagMap: ReplicationLagMap): ReplicationLagMap = {
      explicitReplicasMapping.flatMap { case (token, slaves) =>
        slaves.flatMap { slave =>
          fetchReplicationLagValue(token, slave).map((token, slave) -> _)
        }
      }.toMap
    }

    // Fetch replication lags for all slaves of a given service member
    def updateMemberReplicationLags(token: Long)(currentLagMap: ReplicationLagMap): ReplicationLagMap = {
      explicitReplicasMapping.get(token) match {
        case Some(slaves) =>
          currentLagMap ++ slaves.flatMap { slave =>
            fetchReplicationLagValue(token, slave).map((token, slave) -> _)
          }.toMap
        case None => currentLagMap.filterKeys(_._1 == token)
      }
    }

    // Fetch replication lag for a given slave on a given shard
    def updateSlaveReplicationLag(token: Long, slave: Node)(currentLagMap: ReplicationLagMap): ReplicationLagMap = {
      fetchReplicationLagValue(token, slave)  match {
        case Some(value) => currentLagMap + ((token, slave) -> value)
        case None => currentLagMap - ((token, slave))
      }
    }

    // Fetch replication lag for a given slave, watch for changes, and for node creation if doesn't exist
    private def fetchReplicationLagValue(token: Long, slave: Node): Option[Int] = {
      node2string(slave) match {
        case Some(nodeString) => {
          val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, nodeString)

          // Register a callback triggered when the Zookeeper node will be either created or deleted
          val statusCallback = lagStatusChangedCallbacks.get(path).getOrElse {
            val newCallback = (e: NodeStatusChanged) => lagMapAgent.send(updateSlaveReplicationLag(token, slave) _)
            lagStatusChangedCallbacks += (path -> newCallback)
            newCallback
          }
          zk.exists(path, Some(statusCallback)) match {
            case true => {
              val valueCallback = lagValueChangedCallbacks.get(path).getOrElse {
                val newCallback = (e: NodeValueChanged) => lagMapAgent.send(updateSlaveReplicationLag(token, slave) _)
                lagValueChangedCallbacks += (path -> newCallback)
                newCallback
              }

              try {
                Some(zk.getInt(path, Some(valueCallback)))
              } catch {
                // Node has been deleted between exists() and getInt() calls: will be handled by the status callback
                case e: KeeperException if e.code == Code.NONODE => None
                case t: Throwable => throw t
              }
            }
            case false => None
          }
        }
        case None => {
          warn("Couldn't fetch replication lag for replica {} because it doesn't have a known string representation in Zookeeper")
          None
        }
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

        node2string(node) match {
          case Some(nodeString) => {
            val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, nodeString)

            // Persist value in Zookeeper
            try {
              zk.set(path, lag)
            } catch {
              // Node doesn't exist, create it
              case e: KeeperException if e.code == Code.NONODE => zk.ensureAllExists(path, lag, CreateMode.PERSISTENT)
              case t: Throwable => throw t
            }

            currentLagMap + ((token, node) -> lag)
          }
          case None => {
            warn("Couldn't persist replication lag value {} because node {} doesn't have a known string representation in Zookeeper", lag, node)
            currentLagMap
          }
        }

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

  // The replicas mapping is handled by an Agent.
  // The ReplicasMappingPersistence object defines all the update methods that can be passed to this agent.
  private[this] object ReplicasMappingPersistence {

    val replicasMappingAgent = Agent(ReplicasMapping())

    // Callbacks must be instantiated once then cached so that ZookeeperClient doesn't create watch duplicates
    private var replicasCallbacks: Map[String, NodeEvent => Unit] = Map()

    // Fetches the entire (token -> replicas) mapping for the service.
    def updateReplicasMapping(currentReplicasMapping: ReplicasMapping): ReplicasMapping = {
      val replicasAsTuples = service.members.map { member =>
        // Get the node value from Zookeeper and watch for changes
        val replicas = fetchMemberReplicasList(member)

        member.token -> replicas
      }

      ReplicasMapping(
        replicas = replicasAsTuples.map { case (token, tuples) =>
          (token -> tuples.map(_._1))
        }.toMap,
        nodes = replicasAsTuples.flatMap { case (_, tuples) =>
          tuples
        }.toMap
      )
    }

    def updateMemberReplicas(member: ServiceMember)(currentReplicasMapping: ReplicasMapping): ReplicasMapping = {
      val replicas = fetchMemberReplicasList(member)

      currentReplicasMapping.copy(
        replicas = currentReplicasMapping.replicas + (member.token -> replicas.map(_._1)),
        nodes = currentReplicasMapping.nodes ++ replicas.toMap
      )
    }

    // Fetch list of replicas for the token associated to the given service member, if it exists in Zk.
    // Otherwise, create a watcher to be notified when the replicas are set.
    private def fetchMemberReplicasList(member: ServiceMember): List[(Node, String)] = {
      val replicasPath = ZookeeperClusterManager.zkMemberReplicasPath(service.name, member.token)

      val callback = replicasCallbacks.get(replicasPath).getOrElse {
        val newCallback = (e: NodeEvent) => updateMemberReplicasThenLags(member)
        replicasCallbacks += (replicasPath -> newCallback)
        newCallback
      }

      zk.exists(replicasPath, Some(callback)) match {
        case true =>
          try {
            val replicas = zk.getChildren(replicasPath, Some(callback))
            replicas.map { s =>
              (nodeFromStringNoProtocol(s), s)
            }.toList
          } catch {
            // Node has been deleted between exists() and getString() calls: will be handled by the status callback
            case e: KeeperException if e.code == Code.NONODE => Nil
            case t: Throwable => throw t
          }
        case false => Nil
      }
    }
  }

  private def updateMemberReplicasThenLags(member: ServiceMember) {
    val future = replicasMappingAgent.alter(updateMemberReplicas(member) _)
    future.onSuccess {
      case _ => lagMapAgent.send(updateMemberReplicationLags(member.token) _)
    }
  }

  // Converts a node to its string representation as initially found in Zookeeper
  private def node2string(node: Node): Option[String] = replicasMappingAgent().nodes.get(node)
}

object ZookeeperConsistencyPersistence {

  // Maps a token to its replicas as well as a node to its string representation in Zk
  case class ReplicasMapping(replicas: Map[Long, List[Node]] = Map(), nodes: Map[Node, String] = Map())

  // Maps a token and a replica to a replication lag value
  type ReplicationLagMap = Map[(Long, Node), Int]

  def nodeFromStringNoProtocol(hostnamePort: String): Node = {
    val Array(host, port) = hostnamePort.split(":")
    new Node(host, Map("nrv" -> port.toInt))
  }
}
