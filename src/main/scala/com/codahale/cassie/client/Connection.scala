package com.codahale.cassie.client

import java.net.InetSocketAddress
import org.apache.cassandra.thrift.Cassandra.Client

case class Connection(address: InetSocketAddress, client: Client)
