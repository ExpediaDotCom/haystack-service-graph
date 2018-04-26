package com.expedia.www.haystack.service.graph.graph.builder.writer

import com.expedia.www.haystack.service.graph.graph.builder.model.ServiceGraph

trait ServiceGraphWriter extends AutoCloseable {
  def write(taskId: String, serviceGraph: ServiceGraph)
}
