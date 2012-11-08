package com.wajam.nrv.zookeeper.service

/**
 * Compnion object to deal with zookeeper service paths
 */
object ZookeeperService {
  /**
   * Returns specified service path
   */
  def path(serviceName: String) = "/services/%s".format(serviceName)

  /**
   * Returns specified service data path.
   */
  def dataPath(serviceName: String) = "/services/%s/data".format(serviceName)

  /**
   * Returns service members path
   */
  def membersPath(serviceName: String) = "/services/%s/members".format(serviceName)

  /**
   * Returns specified service member path
   */
  def memberPath(serviceName: String, token: Long) = "/services/%s/members/%d".format(serviceName, token)

  /**
   * Returns specified service member data path.
   */
  def memberDataPath(serviceName: String, token: Long) = "/services/%s/members/%d/data".format(serviceName, token)
}
