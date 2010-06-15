package com.codahale.cassie.client

import java.net.InetSocketAddress
import scala.collection.mutable

/**
 * A host selector which connects to all nodes in a cluster in round-robin
 * fashion, but removes an address from rotation when it is considered to be
 * failed.
 *
 * @author coda
 */
class FailoverAwareRoundRobinHostSelector(clusterMap: ClusterMap) 
  extends FailoverAwareHostSelector {
  private var hosts  = makeHostsIterator
  private val failed = mutable.Set[InetSocketAddress]()

  def next = synchronized { hosts.next() }
  def failed(address: InetSocketAddress): Unit = {
    synchronized {
      failed += address
      hosts   = makeHostsIterator
    }
  }
  def isFailed(address: InetSocketAddress): Boolean = {
    failed.contains(address)
  }

  private def makeHostsIterator: Iterator[InetSocketAddress] = {
    Iterator.continually((clusterMap.hosts -- failed).toArray.iterator).flatten
  }
}
