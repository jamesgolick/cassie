package com.codahale.cassie.client

import org.apache.commons.pool.BasePoolableObjectFactory
import org.apache.thrift.transport.TSocket
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.protocol.TBinaryProtocol
import com.codahale.logula.Logging

/**
 * Creates Cassandra connections for the nodes specified by a HostSelector.
 *
 * @author coda
 */
class FailoverAwarePooledClientFactory(selector: FailoverAwareHostSelector)
        extends BasePoolableObjectFactory with Logging {

  override def validateObject(obj: Any): Boolean = {
    obj match {
      case connection: Connection =>
        log.finer("Validating connection: %s", connection.client)
        if (selector.isFailed(connection.address)) { return false; }

        try {
          connection.client.describe_version()
          true
        } catch {
          case e: Exception =>
            log.warning(e, "Bad connection: %s", connection.client)
            false
        }
      case _ => false
    }
  }

  override def destroyObject(obj: Any) = {
    obj match {
      case connection: Connection =>
        log.finer("Closing connection: %s", connection)
        val transport = connection.client.getOutputProtocol.getTransport
        if (transport.isOpen) {
          transport.close()
        }
    }
  }

  def makeObject = {
    val host = selector.next
    log.finer("Opening connection: %s", host)
    val socket = new TSocket(host.getHostName, host.getPort)
    socket.open()
    Connection(host, new Client(new TBinaryProtocol(socket)))
  }
}
