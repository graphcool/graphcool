package com.prisma.websocket.metrics

import com.prisma.errors.BugsnagErrorReporter
import com.prisma.metrics.MetricsManager
import com.prisma.profiling.MemoryProfiler

object SubscriptionWebsocketMetrics extends MetricsManager(BugsnagErrorReporter(sys.env.getOrElse("BUGSNAG_API_KEY", ""))) {
  MemoryProfiler.schedule(this)

  override def serviceName = "SubscriptionWebsocketService"

  val activeWsConnections              = defineGauge("activeWsConnections")
  val incomingWebsocketMessageRate     = defineCounter("incomingWebsocketMessageRate")
  val outgoingWebsocketMessageRate     = defineCounter("outgoingWebsocketMessageRate")
  val incomingResponseQueueMessageRate = defineCounter("incomingResponseQueueMessageRate")
}
