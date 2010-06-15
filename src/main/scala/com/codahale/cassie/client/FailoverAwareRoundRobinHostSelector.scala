package com.codahale.cassie.client

import java.net.InetSocketAddress
import scala.collection.mutable

class NoLiveHostsException(message: String) extends Exception(message)

/**
 * A host selector which connects to all nodes in a cluster in round-robin
 * fashion, but removes an address from rotation when it is considered to be
 * failed.
 *
 * @author coda
 */
class FailoverAwareRoundRobinHostSelector(clusterMap: ClusterMap) 
  extends FailoverAwareHostSelector {
  private val failed = mutable.Set[InetSocketAddress]()
  private var hosts  = makeHostsIterator

  def next = {
    try {
      synchronized { hosts.next() }
    } catch {
      case e: NoSuchElementException => 
      throw new NoLiveHostsException("No live hosts in %s.".format(clusterMap.hosts))
    }
  }
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
    val liveHosts = clusterMap.hosts -- failed

    if (liveHosts.isEmpty) {
      Iterator[InetSocketAddress]()
    } else {
      Iterator.continually(liveHosts.toArray.iterator).flatten
    }
  }
}
