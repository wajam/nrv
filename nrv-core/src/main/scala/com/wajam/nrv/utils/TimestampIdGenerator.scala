package com.wajam.nrv.utils

/**
 * Generate unique long id based on the current time. Can generate up to 10K unique id if invoked in
 * the same millisecond. This class is not thread safe and must be invoked from a single thread, synchronized
 * externally or used with SynchronizedIdGenerator
 */
@deprecated(message = "Use com.wajam.commons.TimestampIdGenerator", since = "2014-05-14 14:37:31")
class TimestampIdGenerator extends com.wajam.commons.TimestampIdGenerator
