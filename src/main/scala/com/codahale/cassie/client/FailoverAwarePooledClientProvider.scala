package com.codahale.cassie.client

import org.apache.commons.pool.impl.GenericObjectPool

/**
 * A client provider which provides access to a pool of connections to nodes
 * throughout the cluster, and manages failover scenarios.
 *
 * @author jamesgolick
 */
class FailoverAwarePooledClientProvider(val selector: FailoverAwareHostSelector,
                                        minIdle: Int,
                                        maxActive: Int,
                                        val maxRetry: Int)
        extends FailoverAwareRetryingClientProvider {

  private val factory = new FailoverAwarePooledClientFactory(selector)
  private val config = {
    val c = new GenericObjectPool.Config
    c.maxActive = maxActive
    c.maxIdle = maxActive
    c.minIdle = minIdle
    c.testWhileIdle = true
    c
  }
  protected val pool = new GenericObjectPool(factory, config)
}
