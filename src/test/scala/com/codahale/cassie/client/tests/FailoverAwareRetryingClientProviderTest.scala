package com.codahale.cassie.client.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.apache.commons.pool.ObjectPool
import org.mockito.Mockito.{when, verify}
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.transport.TTransportException
import org.scalatest.{OneInstancePerTest, Spec}
import com.codahale.cassie.client.Connection
import com.codahale.cassie.client.FailoverAwareRetryingClientProvider
import com.codahale.cassie.client.FailoverAwareHostSelector
import java.util.logging.{Logger, Level}
import java.io.IOException
import java.net.InetSocketAddress

class FailoverAwareRetryingClientProviderTest 
  extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest {
  Logger.getLogger("com.codahale.cassie.client.tests.FailoverAwareRetryingClientProviderTest").setLevel(Level.OFF)

  def connection(value: String): Connection = {
    val client = mock[Client]
    when(client.describe_version).thenReturn(value)
    val address = mock[InetSocketAddress]
    Connection(address, client)
  }

  def connection(exception: Exception): Connection = {
    val client = mock[Client]
    when(client.describe_version).thenThrow(exception)
    val address = mock[InetSocketAddress]
    Connection(address, client)
  }

  describe("retrying a command") {
    val connection1 = connection("one")
    val connection2 = connection("two")
    val connection3 = connection(new TTransportException("OH GOD"))
    val connection4 = connection("four")

    val hostSelector = mock[FailoverAwareHostSelector]
    val mockPool     = mock[ObjectPool]

    when(mockPool.borrowObject).thenReturn(connection1, connection2, connection3,
      connection4, connection3, connection3, connection3)

    val provider = new FailoverAwareRetryingClientProvider {
      val selector = hostSelector
      val pool     = mockPool
      val maxRetry = 2
    }

    it("retries queries") {
      1.to(3).map { _ => provider.map { c => c.describe_version } } must equal(List("one", "two", "four"))
    }

    it("surfaces connection exceptions when a maximum of errors has been met") {
      evaluating {
        1.to(6).map { _ => provider.map { c => c.describe_version } }
      } must produce[IOException]      
    }

    it("marks failed connections as failed in the host selector") {
      1.to(3).map { _ => provider.map { c => c.describe_version } }
      verify(hostSelector).failed(connection3.address)
    }
  }
}
