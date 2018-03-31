package com.expedia.www.haystack.commons.kstreams.app

trait ManagedLifeCycle {
  def start()

  def stop()

  def isRunning : Boolean
}
