package com.codahale.cassie.client

import java.net.InetSocketAddress

/**
 * A host selector that accepts node failure feedback. The theory here is that
 * the ClientProvider will measure failures and indicate them to the HostSelector
 * so that it stops returning that address.
 *
 * Also provides a query method so that an ObjectFactory can validate its objects
 * by checking whether the HostSelector currently believes that they are failed.
 *
 * @author jamesgolick
 */
trait FailoverAwareHostSelector extends HostSelector {
  def failed(address: InetSocketAddress): Unit
  def isFailed(address: InetSocketAddress): Boolean
}
