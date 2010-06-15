package com.codahale.cassie.client.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.codahale.cassie.client.ClusterMap
import com.codahale.cassie.client.FailoverAwareRoundRobinHostSelector
import org.mockito.Mockito.when
import java.net.InetSocketAddress
import com.codahale.logula.Log
import java.util.logging.Level
import org.scalatest.{BeforeAndAfterAll, Spec}
import com.codahale.cassie.tests.util.MockCassandraServer

class FailoverAwareRoundRobinHostSelectorTest extends Spec
        with MustMatchers with MockitoSugar with BeforeAndAfterAll {
  val clusterMap = mock[ClusterMap]
  val cassandra1 = addr("cassandra1.local", 9160)
  val cassandra2 = addr("cassandra2.local", 9160)
  val hosts      = Set(cassandra1, cassandra2)
  when(clusterMap.hosts).thenReturn(hosts)

  describe("when no nodes are failed") {
    var selector = new FailoverAwareRoundRobinHostSelector(clusterMap)

    it("alternates hosts in order") {
      selector.next must equal(cassandra1)
      selector.next must equal(cassandra2)
      selector.next must equal(cassandra1)
      selector.next must equal(cassandra2)
    }

    it("correctly returns the failure status of nodes in the cluster") {
      selector.isFailed(cassandra1) must equal(false)
      selector.isFailed(cassandra2) must equal(false)
    }
  }

  describe("when a node is failed") {
    var selector = new FailoverAwareRoundRobinHostSelector(clusterMap)
    selector.failed(cassandra1)

    it("only returns the working hosts") {
      selector.next must equal(cassandra2)
      selector.next must equal(cassandra2)
      selector.next must equal(cassandra2)
    }

    it("correctly returns the failure status of nodes in the cluster") {
      selector.isFailed(cassandra1) must equal(true)
      selector.isFailed(cassandra2) must equal(false)
    }
  }

  def addr(host: String, port: Int) = new InetSocketAddress(host, port)
}
