package com.codahale.cassie.client.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.apache.cassandra.thrift.Cassandra.Client
import org.mockito.Mockito.{when, verify, never}
import org.apache.thrift.TException
import com.codahale.logula.Log
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.TTransport
import com.codahale.cassie.client.Connection
import com.codahale.cassie.client.FailoverAwareHostSelector
import com.codahale.cassie.client.FailoverAwarePooledClientFactory
import org.scalatest.{BeforeAndAfterAll, Spec}
import java.net.InetSocketAddress
import java.util.logging.Level
import com.codahale.cassie.tests.util.MockCassandraServer

class FailoverAwarePooledClientFactoryTest
  extends Spec with MustMatchers with MockitoSugar with BeforeAndAfterAll {
  Log.forClass(classOf[FailoverAwarePooledClientFactory]).level = Level.OFF

  val server = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server.cassandra.describe_version).thenReturn("moof")
  val socket = new InetSocketAddress("localhost", 9160)

  override protected def beforeAll() {
    server.start()
  }

  override protected def afterAll() {
    server.stop()
  }

  describe("validating a working client connection") {
    val client     = mock[Client]
    val connection = Connection(socket, client)
    val selector   = mock[FailoverAwareHostSelector]
    when(selector.isFailed(connection.address)).thenReturn(false)
    val factory    = new FailoverAwarePooledClientFactory(selector)

    val result     = factory.validateObject(connection)

    it("calls describe_version") {
      verify(client).describe_version
    }

    it("calls isFailed") {
      verify(selector).isFailed(connection.address)
    }

    it("returns true") {
      result must be(true)
    }
  }

  describe("validating a broken client connection") {
    val client     = mock[Client]
    when(client.describe_version).thenThrow(new TException("NO YUO"))
    val connection = Connection(socket, client)
    val selector   = mock[FailoverAwareHostSelector]
    when(selector.isFailed(connection.address)).thenReturn(false)
    val factory    = new FailoverAwarePooledClientFactory(selector)
    
    it("calls isFailed") {
      factory.validateObject(connection)

      verify(selector).isFailed(connection.address)
    }

    it("returns false") {
      factory.validateObject(connection) must be(false)
    }
  }

  describe("validating a connection a failed node") {
    val client     = mock[Client]
    val connection = Connection(socket, client)
    val selector   = mock[FailoverAwareHostSelector]
    when(selector.isFailed(connection.address)).thenReturn(true)
    val factory    = new FailoverAwarePooledClientFactory(selector)

    it("calls isFailed") {
      factory.validateObject(connection)

      verify(selector).isFailed(connection.address)
    }

    it("returns false") {
      factory.validateObject(connection) must be(false)
    }
  }

  describe("validating a random object") {
    val selector = mock[FailoverAwareHostSelector]
    val factory = new FailoverAwarePooledClientFactory(selector)

    it("returns false") {
      factory.validateObject("HI THERE") must be(false)
    }
  }

  describe("destroying an open client connection") {
    val transport = mock[TTransport]
    when(transport.isOpen).thenReturn(true)

    val protocol = mock[TProtocol]
    when(protocol.getTransport).thenReturn(transport)

    val client = mock[Client]
    when(client.getOutputProtocol).thenReturn(protocol)

    val connection = mock[Connection]
    when(connection.client).thenReturn(client)

    val selector = mock[FailoverAwareHostSelector]
    val factory = new FailoverAwarePooledClientFactory(selector)

    it("closes the connection") {
      factory.destroyObject(connection)

      verify(transport).close()
    }
  }

  describe("destroying a closed client connection") {
    val transport = mock[TTransport]
    when(transport.isOpen).thenReturn(false)

    val protocol = mock[TProtocol]
    when(protocol.getTransport).thenReturn(transport)

    val client = mock[Client]
    when(client.getOutputProtocol).thenReturn(protocol)

    val connection = mock[Connection]
    when(connection.client).thenReturn(client)

    val selector = mock[FailoverAwareHostSelector]
    val factory = new FailoverAwarePooledClientFactory(selector)

    it("does not re-close the connection") {
      factory.destroyObject(connection)

      verify(transport, never).close()
    }
  }

  describe("creating a new connection") {
    val selector = mock[FailoverAwareHostSelector]
    val address  = new InetSocketAddress("127.0.0.1", server.port)
    when(selector.next).thenReturn(address)

    val factory = new FailoverAwarePooledClientFactory(selector)

    it("connects to the next node provided by the host selector") {
      val connection = factory.makeObject.asInstanceOf[Connection]
      connection.client.describe_version must equal("moof")
      connection.address must equal(address)
    }
  }
}
