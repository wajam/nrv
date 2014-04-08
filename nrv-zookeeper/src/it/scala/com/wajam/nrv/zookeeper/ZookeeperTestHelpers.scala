package com.wajam.nrv.zookeeper

import com.wajam.nrv.cluster.{Node, ServiceMemberVote}
import com.wajam.nrv.service.{Service, MemberStatus, ServiceMember}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.nrv.zookeeper.ZookeeperClient._
import org.apache.zookeeper.CreateMode

trait ZookeeperTestHelpers {
  def zk: ZookeeperClient

  def zkCreateService(service: Service) {
    val path = ZookeeperClusterManager.zkServicePath(service.name)
    zk.ensureAllExists(path, service.name, CreateMode.PERSISTENT)
  }

  def zkCreateServiceMember(service: Service, serviceMember: ServiceMember) {
    val path = ZookeeperClusterManager.zkMemberPath(service.name, serviceMember.token)
    val created = zk.ensureAllExists(path, serviceMember.toString, CreateMode.PERSISTENT)

    // if node existed, overwrite
    if (!created) {
      zk.set(path, serviceMember.toString)
    }

    val votePath = ZookeeperClusterManager.zkMemberVotesPath(service.name, serviceMember.token)
    zk.ensureAllExists(votePath, "", CreateMode.PERSISTENT)
  }

  def zkGetServiceMember(service: Service, token: Long): ServiceMember = {
    val path = ZookeeperClusterManager.zkMemberPath(service.name, token)
    val node = zk.getString(path)
    ServiceMember.fromString(node)
  }

  def zkDeleteServiceMember(service: Service, serviceMember: ServiceMember) {
    val path = ZookeeperClusterManager.zkMemberPath(service.name, serviceMember.token)
    zk.deleteRecursive(path)
  }

  def zkCastVote(service: Service, candidateMember: ServiceMember, voterMember: ServiceMember, votedStatus: MemberStatus) {
    val vote = new ServiceMemberVote(candidateMember, voterMember, votedStatus)
    val path = ZookeeperClusterManager.zkMemberVotePath(service.name, candidateMember.token, voterMember.token)
    val created = zk.ensureAllExists(path, vote.toString, CreateMode.PERSISTENT)

    // if node existed, overwrite
    if (created) {
      zk.set(path, vote.toString)
    }
  }

  def zkDeleteVote(service: Service, candidateMember: ServiceMember, voterMember: ServiceMember) {
    val path = ZookeeperClusterManager.zkMemberVotePath(service.name, candidateMember.token, voterMember.token)
    zk.delete(path)
  }

  def zkAddReplica(service: Service, token: Long, nodeString: String) {
    val path = ZookeeperClusterManager.zkMemberReplicasPath(service.name, token) + "/" + nodeString

    zk.ensureAllExists(path, Array(), CreateMode.PERSISTENT)
  }

  def zkRemoveReplica(service: Service, token: Long, nodeString: String) {
    val path = ZookeeperClusterManager.zkMemberReplicasPath(service.name, token) + "/" + nodeString

    zk.delete(path)
  }

  def zkCreateReplicationLag(service: Service, token: Long, nodeString: String, lag: Int) {
    val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, nodeString)

    zk.ensureAllExists(path, lag, CreateMode.PERSISTENT)
  }

  def zkUpdateReplicationLag(service: Service, token: Long, nodeString: String, lag: Int) {
    val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, nodeString)

    zk.set(path, lag)
  }

  def zkRemoveReplicationLag(service: Service, token: Long, nodeString: String) {
    val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, nodeString)

    zk.delete(path)
  }

  def zkGetReplicationLag(service: Service, token: Long, nodeString: String): Int = {
    val path = ZookeeperClusterManager.zkMemberReplicaLagPath(service.name, token, nodeString)

    zk.getInt(path)
  }
}
