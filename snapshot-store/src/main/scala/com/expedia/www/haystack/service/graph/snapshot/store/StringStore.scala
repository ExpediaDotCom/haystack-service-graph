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

import java.nio.file.Path
import java.time.Instant

trait StringStore {
  /**
    * Write a byte array to the persistent store
    *
    * @param instant date/time of the write, used to create the name, which will later be used in read() and purge()
    * @param content String to write
    * @return the Path to which the content was written
    */
  def write(instant: Instant, content: String): Path

  /**
    * Read a byte array from the persistent store
    *
    * @param instant date/time of the read
    * @return the byte array of the youngest item whose ISO-8601-based name is earlier or equal to instant
    */
  def read(instant: Instant): Option[String]

  /**
    * Purges items from the persistent store
    *
    * @param instant date/time of items to be purged; items whose ISO-8601-based name is earlier or equals to instant
    *                will be purged
    * @return the number of items purged
    */
  def purge(instant: Instant): Integer
}
