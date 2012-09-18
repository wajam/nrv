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
  protected var _traceFilter: TraceFilter = null

  def cluster: Cluster =
    if (_cluster != null)
      this._cluster
    else if (this.supporter != null)
      this.supporter.cluster
    else
      throw new UninitializedError

  def service: Service =
    if (_service != null)
      this._service
    else if (this.supporter != null)
      this.supporter.service
    else
      throw new UninitializedError

  def resolver: Resolver =
    if (_resolver != null)
      this._resolver
    else if (this.supporter != null)
      this.supporter.resolver
    else
      throw new UninitializedError

  def protocol: Protocol =
    if (_protocol != null)
      this._protocol
    else if (this.supporter != null)
      this.supporter.protocol
    else
      throw new UninitializedError

  def switchboard: Switchboard =
    if (_switchboard != null)
      this._switchboard
    else if (this.supporter != null)
      this.supporter.switchboard
    else
      throw new UninitializedError

  def traceFilter: TraceFilter =
    if (_traceFilter != null)
      this._traceFilter
    else if (this.supporter != null)
      this.supporter.traceFilter
    else
      throw new UninitializedError

  def checkSupported() {
    if (this.cluster == null || this.service == null || this.protocol == null || this.resolver == null || this.switchboard == null || this.traceFilter == null)
      throw new UninitializedError
  }

  def applySupport(cluster: Option[Cluster] = None, service: Option[Service] = None, resolver: Option[Resolver] = None, protocol: Option[Protocol] = None,
                   switchboard: Option[Switchboard] = None, traceFilter: Option[TraceFilter] = None) {
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

    if (traceFilter != None)
      this._traceFilter = traceFilter.get
  }

  def supportedBy(supporter: ActionSupport) {
    this.supporter = supporter
  }
}
