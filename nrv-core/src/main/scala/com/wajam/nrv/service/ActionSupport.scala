package com.wajam.nrv.service

import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.cluster.Cluster

/**
 * Action support trait handles protocol/resolver/... switching
 * for each action in a hierarchic fashion to handle defaults.
 *
 * Ex: The cluster implements this trait to give default protocol/resolver
 * to services that implement it to give defaults to actions.
 */
trait ActionSupport {
  private var supporter: ActionSupport = null

  protected var _cluster: Cluster = null
  protected var _service: Service = null
  protected var _resolver: Resolver = null
  protected var _protocol: Protocol = null
  protected var _switchboard: Switchboard = null

  def cluster: Cluster =
    if (_cluster != null)
      this._cluster
    else
      this.supporter.cluster

  def service: Service =
    if (_service != null)
      this._service
    else
      this.supporter.service

  def resolver: Resolver =
    if (_resolver != null)
      this._resolver
    else
      this.supporter.resolver

  def protocol: Protocol =
    if (_protocol != null)
      this._protocol
    else
      this.supporter.protocol

  def switchboard: Switchboard =
    if (_switchboard != null)
      this._switchboard
    else
      this.supporter.switchboard

  def checkSupported() {
    if (this.cluster == null || this.service == null || this.protocol == null || this.resolver == null || this.switchboard == null)
      throw new UninitializedError
  }

  def applySupport(cluster: Option[Cluster] = None, service: Option[Service] = None, resolver: Option[Resolver] = None, protocol: Option[Protocol] = None, switchboard: Option[Switchboard] = None) {
    if (cluster != None)
      this._cluster = cluster.get

    if (service != None)
      this._service = service.get

    if (resolver != None)
      this._resolver = resolver.get

    if (protocol != None)
      this._protocol = protocol.get

    if (switchboard != None)
      this._switchboard = switchboard.get
  }

  def supportedBy(supporter: ActionSupport) {
    this.supporter = supporter
  }
}
