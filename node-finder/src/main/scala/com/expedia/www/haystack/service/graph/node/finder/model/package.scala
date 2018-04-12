package com.expedia.www.haystack.service.graph.node.finder

import scala.collection.mutable

package object model {

  /**
    * Simple trait for any class that has a weight of some sort
    */
  trait Weighable {
    def weight(): Long
  }

  type BottomHeavyHeap[T <: Weighable] = mutable.PriorityQueue[T]

  /**
    * Priority Queue or a Heap data structure where the "lightest" element is at the top (i.e., bottom-heavy heap)
    */
  object BottomHeavyHeap {
    def apply[T <: Weighable]() = new BottomHeavyHeap[T]()((x: Weighable, y: Weighable) => (y.weight() - x.weight()).asInstanceOf[Int])
  }
}
