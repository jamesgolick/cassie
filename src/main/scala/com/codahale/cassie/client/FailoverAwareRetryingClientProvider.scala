package com.codahale.cassie.client

import org.apache.commons.pool.ObjectPool
import com.codahale.logula.Logging
import java.io.IOException
import org.apache.thrift.transport.TTransportException
import org.apache.cassandra.thrift.Cassandra.{Client, Iface}
import org.apache.cassandra.thrift.TimedOutException

/**
 * A client provider which retries commands, potentially recovering from dropped
 * network connections, etc. When it fails to execute a command on a node, it 
 * notifies the host selector that it is failed.
 *
 * @author jamesgolick
 */
abstract class FailoverAwareRetryingClientProvider
  extends ClientProvider with Logging {
  protected val selector: FailoverAwareHostSelector
  protected val pool:     ObjectPool
  protected val maxRetry: Int

  def map[A](f: (Iface) => A): A = {
    var attempts = 0
    var lastError: Throwable = null
    while (attempts < maxRetry) {
      if (attempts > 0) {
        log.warning("Re-attempting command, try %d", attempts)
      } else {
        log.finer("Acquiring connection")
      }
      val connection = pool.borrowObject().asInstanceOf[Connection]
      try {
        val result = f(connection.client)
        log.finer("Returning connection")
        pool.returnObject(connection)
        return result
      } catch {
        // We're not catching UnavailableException because it means your cluster
        // is effed and that's not something a retry is going to help. We're
        // also not catching AuthenticationException, AuthorizationException,
        // InvalidRequestException, or NotFoundException for what should be
        // pretty obvious reasons.
        case e: TTransportException => {
          lastError = recoverFrom(e, attempts, connection)
        }
        case e: TimedOutException => {
          lastError = recoverFrom(e, attempts, connection)
        }
      }
      attempts += 1
    }
    throw new IOException("Aborting query after %d attempts".format(attempts), lastError)
  }

  private def recoverFrom(e: Exception, attempts: Int, connection: Connection) = {
    log.warning(e, "Attempt %d failed", attempts)
    selector.failed(connection.address)
    pool.invalidateObject(connection)
    e
  }
}
