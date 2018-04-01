package com.expedia.www.haystack.commons.serde

import com.expedia.www.haystack.UnitTestSpec
import com.expedia.www.haystack.commons.kstreams.serde.SpanSerde

class SpanSerdeSpec extends UnitTestSpec {
  describe("span serializer") {
    it("should serialize a span") {
      Given("a span serializer")
      val serializer = (new SpanSerde).serializer
      And("a valid span is provided")
      val span = newSpan("foo", "bar", 100, client = true, server = false)
      When("span serializer is used to serialize the span")
      val bytes = serializer.serialize("proto-spans", span)
      Then("it should serialize the object")
      bytes.nonEmpty should be(true)
    }
  }
  describe("span deserializer") {
    it("should deserialize a span") {
      Given("a span deserializer")
      val serializer = (new SpanSerde).serializer
      val deserializer = (new SpanSerde).deserializer
      And("a valid span is provided")
      val span = newSpan("foo", "bar", 100, client = true, server = false)
      When("span deserializer is used on valid array of bytes")
      val bytes = serializer.serialize("proto-spans", span)
      val span2 = deserializer.deserialize("proto-spans", bytes)
      Then("it should deserialize correctly")
      span should be (span2)
    }
  }
}
