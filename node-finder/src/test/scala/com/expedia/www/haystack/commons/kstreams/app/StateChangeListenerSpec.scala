package com.expedia.www.haystack.commons.kstreams.app

import com.expedia.www.haystack.UnitTestSpec
import com.expedia.www.haystack.commons.health.HealthStatusController
import org.easymock.EasyMock._

class StateChangeListenerSpec extends UnitTestSpec {
  describe("StateChangeListener") {
    it("should set the health status to healthy when requested") {
      Given("a valid instance of StateChangeListener")
      val healthStatusController = mock[HealthStatusController]
      val stateChangeListener = new StateChangeListener(healthStatusController)
      When("set healthy is invoked")
      expecting {
        healthStatusController.setHealthy().once()
      }
      replay(healthStatusController)
      stateChangeListener.state(true)
      Then("it should set health status to healthy")
      verify(healthStatusController)
    }
    it("should set the health status to unhealthy when requested") {
      Given("a valid instance of StateChangeListener")
      val healthStatusController = mock[HealthStatusController]
      val stateChangeListener = new StateChangeListener(healthStatusController)
      When("set unhealthy is invoked")
      expecting {
        healthStatusController.setUnhealthy().once()
      }
      replay(healthStatusController)
      stateChangeListener.state(false)
      Then("it should set health status to healthy")
      verify(healthStatusController)
    }
    it("should set application status to unhealthy when an un caught exception is raised") {
      Given("a valid instance of StateChangeListener")
      val healthStatusController = mock[HealthStatusController]
      val stateChangeListener = new StateChangeListener(healthStatusController)
      val exception = new IllegalArgumentException
      val thread = new Thread("Thread-1")
      When("an uncaught exception is raised")
      expecting {
        healthStatusController.setUnhealthy().once()
      }
      replay(healthStatusController)
      stateChangeListener.uncaughtException(thread, exception)
      Then("it should set the status to unhealthy")
      verify(healthStatusController)
    }
  }

}
