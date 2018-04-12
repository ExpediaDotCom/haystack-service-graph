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
