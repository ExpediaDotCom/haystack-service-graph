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
package com.expedia.www.haystack.service.graph.node.finder.model

import com.expedia.www.haystack.TestSpec

class BottomHeavyHeapSpec extends TestSpec {
  describe("a bottom-heavy heap implementation") {
    it("should store objects that are heavy at the bottom of the tree") {
      Given("a bottom heavy heap")
      val heap = BottomHeavyHeap[WeighableObject]()
      And("elements are added to it")
      heap.enqueue(WeighableObject(5))
      heap.enqueue(WeighableObject(1))
      heap.enqueue(WeighableObject(10))
      heap.enqueue(WeighableObject(3))
      var actual = Array[Long]()
      When("it is dequeued")
      while (heap.nonEmpty) { actual = actual :+ heap.dequeue().weight }
      Then("it should be returned in the sorted order with lightest element first")
      actual should be (Array[Long](1, 3, 5, 10))
    }
  }

  case class WeighableObject(weight: Long) extends Weighable
}
