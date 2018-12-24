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
import org.scalatest.{FunSpec, Matchers}

class JsonIntoDataFramesTransformerSpec extends FunSpec with Matchers {
  private val stringSnapshotStoreSpecBase = new SnapshotStoreSpecBase

  val jsonIntoDataFramesTransformer = new JsonIntoDataFramesTransformer
  describe("JsonIntoDataFramesTransformer.parseJson()") {
    it("should parse service graph JSON into nodes and edges") {
      val serviceGraphJson = stringSnapshotStoreSpecBase.readFile(JsonFileNameWithExtension)
      val nodesAndEdges = jsonIntoDataFramesTransformer.parseJson(serviceGraphJson)
      nodesAndEdges.nodes shouldEqual stringSnapshotStoreSpecBase.readFile(NodesCsvFileNameWithExtension)
      nodesAndEdges.edges shouldEqual stringSnapshotStoreSpecBase.readFile(EdgesCsvFileNameWithExtension)
    }
  }
}
