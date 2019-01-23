/*
 *
 *     Copyright 2018 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */
package com.expedia.www.haystack.service.graph.snapshotter

import java.io.File
import java.nio.file.{Files, Path}
import java.time.{Clock, Instant}
import java.util.concurrent.TimeUnit

import com.expedia.www.haystack.service.graph.snapshot.store.FileSnapshotStore
import com.expedia.www.haystack.service.graph.snapshotter.Main.{ServiceGraphUrl, StringStoreClassRequiredMsg}
import org.mockito.Matchers.any
import org.mockito.Mockito.{verify, verifyNoMoreInteractions, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.slf4j.Logger
import scalaj.http.{HttpRequest, HttpResponse}

import scala.io.{BufferedSource, Codec, Source}

class MainSpec extends FunSpec with Matchers with MockitoSugar with BeforeAndAfter {
  private var mockLogger: Logger = _
  private var realLogger: Logger = _

  private var mockFactory: Factory = _
  private var realFactory: Factory = _

  private var mockClock: Clock = _
  private var realClock: Clock = _

  private val mockHttpRequest = mock[HttpRequest]

  private def readFile(fileName: String): String = {
    implicit val codec: Codec = Codec.UTF8
    lazy val bufferedSource: BufferedSource = Source.fromResource(fileName)
    val fileContents = bufferedSource.getLines.mkString("\n")
    bufferedSource.close()
    fileContents + "\n"
  }

  private val body = readFile("serviceGraph.json")
  private val httpResponse: HttpResponse[String] = new HttpResponse[String](body = body, code = 0, headers = Map())
  private val now = Instant.now()

  private var tempDirectory: Path = _

  before {
    saveReaObjectsThatWillBeReplacedWithMocks()
    createMocks()
    replaceRealObjectsWithMocks()

    tempDirectory = Files.createTempDirectory(this.getClass.getSimpleName)

    def saveReaObjectsThatWillBeReplacedWithMocks(): Unit = {
      realLogger = Main.logger
      realFactory = Main.factory
      realClock = Main.clock
    }

    def createMocks(): Unit = {
      mockLogger = mock[Logger]
      mockFactory = mock[Factory]
      mockClock = mock[Clock]
    }

    def replaceRealObjectsWithMocks(): Unit = {
      Main.logger = mockLogger
      Main.factory = mockFactory
      Main.clock = mockClock
    }
  }

  after {
    restoreRealObjects()
    recursiveDelete(tempDirectory.toFile)
    verifyNoMoreInteractions(mockLogger, mockFactory, mockClock)

    def restoreRealObjects(): Unit = {
      Main.logger = realLogger
      Main.factory = realFactory
      Main.clock = realClock
    }

    def recursiveDelete(file: File) {
      if (file.isDirectory)
        Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(recursiveDelete)
      file.delete
    }
  }

  describe("Main.main() called with no arguments") {
    it("should log an error") {
      Main.main(Array())
      verify(mockLogger).error(StringStoreClassRequiredMsg)
    }
  }

  describe("Main.main() called with FileSnapshotStore arguments") {
    it("should create a FileSnapshotStore, write to it, then call purge()") {
      when(mockFactory.createHttpRequest(any(), any())).thenReturn(mockHttpRequest)
      when(mockHttpRequest.asString).thenReturn(httpResponse)
      when(mockClock.instant()).thenReturn(now)

      Main.main(Array(new FileSnapshotStore().getClass.getCanonicalName, tempDirectory.toString))

      verifyDirectoryIsEmptyToProveThatPurgeWasCalled
      verify(mockFactory).createHttpRequest(ServiceGraphUrl, now.toEpochMilli - TimeUnit.HOURS.toMillis(1))
      verify(mockHttpRequest).asString
      verify(mockClock).instant()

      def verifyDirectoryIsEmptyToProveThatPurgeWasCalled = {
        tempDirectory.toFile.listFiles().length shouldBe 0
      }
    }
  }

  describe("Factory.createHttpRequest()") {
    it("should properly construct the URL") {
      val factory = new Factory
      val httpRequest = factory.createHttpRequest(Main.ServiceGraphUrl, now.toEpochMilli)
      val url = httpRequest.url
      url should startWith(Main.ServiceGraphUrlBase)
      url should endWith(Main.ServiceGraphUrlSuffix.format(now.toEpochMilli))
    }
  }

}
