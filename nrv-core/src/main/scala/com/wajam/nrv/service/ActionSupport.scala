package com.wajam.nrv.service

import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.tracing.Tracer
import com.wajam.nrv.consistency.Consistency
import com.wajam.nrv.protocol.codec.Codec

/**
 * ActionSupport trait handles protocol/resolver/... switching
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
  protected var _tracer: Tracer = null
  protected var _consistency: Consistency = null
  protected var _responseTimeout: Option[Long] = None
  protected var _nrvCodec: Option[Codec] = None

  //Actions are bound to a default protocol (i.e. _protocol) but other supported
  //protocol can be used to call the action.
  protected var _supportedProtocols: Option[Set[Protocol]] = None

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

  def tracer: Tracer =
    if (_tracer != null)
      this._tracer
    else if (this.supporter != null)
      this.supporter.tracer
    else
      throw new UninitializedError

  def consistency: Consistency =
    if (_consistency != null)
      this._consistency
    else if (this.supporter != null)
      this.supporter.consistency
    else
      throw new UninitializedError

  def responseTimeout: Long = {
    _responseTimeout match {
      case Some(timeout) => timeout
      case None => {
        if (this.supporter != null) {
          this.supporter.responseTimeout
        } else {
          throw new UninitializedError
        }
      }
    }
  }

  def nrvCodec: Codec = {
    _nrvCodec match {
      case Some(codec) => codec
      case None => {
        if (this.supporter != null) {
          this.supporter.nrvCodec
        } else {
          throw new UninitializedError
        }
      }
    }
  }

  def supportedProtocols: Set[Protocol] = {
    _supportedProtocols.getOrElse {
      if (this.supporter != null) {
        this.supporter.supportedProtocols
      } else {
        throw new UninitializedError
      }
    }
  }

  def checkSupported() {
    this.cluster
    this.service
    this.protocol
    this.resolver
    this.switchboard
    this.tracer
    this.consistency
    this.responseTimeout
    this.nrvCodec
    this.supportedProtocols
  }

  def applySupport(cluster: Option[Cluster] = None,
                   service: Option[Service] = None,
                   resolver: Option[Resolver] = None,
                   protocol: Option[Protocol] = None,
                   switchboard: Option[Switchboard] = None,
                   tracer: Option[Tracer] = None,
                   consistency: Option[Consistency] = None,
                   responseTimeout: Option[Long] = None,
                   nrvCodec: Option[(Codec)] = None,
                   supportedProtocols: Option[Set[Protocol]] = None) {

    cluster.map(this._cluster = _)
    service.map(this._service = _)
    resolver.map(this._resolver = _)
    protocol.map(this._protocol = _)
    switchboard.map(this._switchboard = _)
    tracer.map(this._tracer = _)
    consistency.map(this._consistency = _)
    responseTimeout.map(timeout => this._responseTimeout = Some(timeout))
    nrvCodec.map(codec => this._nrvCodec = Some(codec))
    supportedProtocols.map(supProtocols => this._supportedProtocols = Some(supProtocols))
  }

  def applySupportOptions(options: ActionSupportOptions) {
    this.applySupport(options.cluster, options.service, options.resolver, options.protocol, options.switchboard,
      options.tracer, options.consistency, options.responseTimeout, options.nrvCodec, options.supportedProtocols)
  }

  def supportedBy(supporter: ActionSupport) {
    this.supporter = supporter
  }
}

class ActionSupportOptions(val cluster: Option[Cluster] = None,
                           val service: Option[Service] = None,
                           val resolver: Option[Resolver] = None,
                           val protocol: Option[Protocol] = None,
                           val switchboard: Option[Switchboard] = None,
                           val tracer: Option[Tracer] = None,
                           val consistency: Option[Consistency] = None,
                           val responseTimeout: Option[Long] = None,
                           val nrvCodec: Option[Codec] = None,
                           val supportedProtocols: Option[Set[Protocol]] = None) {
}
