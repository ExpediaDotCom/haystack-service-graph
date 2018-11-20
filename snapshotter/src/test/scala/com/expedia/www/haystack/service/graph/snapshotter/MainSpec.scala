package com.expedia.www.haystack.service.graph.snapshotter

import org.mockito.Mockito
import org.mockito.Mockito.{verify, verifyNoMoreInteractions}
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.mockito.MockitoSugar
import org.slf4j.Logger

class MainSpec extends FunSpec with Matchers with MockitoSugar {
  private val logger = mock[Logger]
  Main.LOGGER = logger

  describe("MainSpec.main") {
    it ("should log an error if no arguments are specified") {
      Main.main(Array())
      verify(logger).error(Main.StringStoreClassRequiredMsg)
      verifyNoMoreInteractions(logger)
    }
    it("should create a FileStoreSpec when told to do so") {
      val array = Array("com.expedia.www.haystack.service.graph.snapshot.store.FileStore", "/var")
      Main.main(array)
      verifyNoMoreInteractions(logger)
    }
  }
}
