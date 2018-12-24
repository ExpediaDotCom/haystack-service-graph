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
package com.expedia.www.haystack.service.graph.snapshot.store

import com.expedia.www.haystack.service.graph.snapshot.store.Constants._
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}
import org.slf4j.Logger

class DataFramesIntoJsonTransformerSpec extends FunSpec with Matchers with MockitoSugar {
  private val stringSnapshotStoreSpecBase = new SnapshotStoreSpecBase
  private val mockLogger = mock[Logger]

  describe("DataFramesIntoJsonTransformerSpec.parseDataFrames()") {
    val dataFramesIntoJsonTransformer = new DataFramesIntoJsonTransformer(mockLogger)
    it("should parse service graph nodes and edges into JSON") {
      val nodesRawData = stringSnapshotStoreSpecBase.readFile(NodesCsvFileNameWithExtension)
      val edgesRawData = stringSnapshotStoreSpecBase.readFile(EdgesCsvFileNameWithExtension)
      val json = dataFramesIntoJsonTransformer.parseDataFrames(nodesRawData, edgesRawData)
      json shouldEqual stringSnapshotStoreSpecBase.readFile(JsonFileNameWithExtension)
      Mockito.verifyNoMoreInteractions(mockLogger)
    }
  }
}
