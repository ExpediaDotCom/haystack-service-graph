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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.{Comparator, Optional}

class FileSnapshotStore(val directoryName: String) extends SnapshotStore {
  private val directory = Paths.get(directoryName)
  protected val pathNameComparator: Comparator[Path] = (o1: Path, o2: Path) => o1.toString.compareTo(o2.toString)

  def this() = {
    this("/")
  }

  /**
    * Returns a FileStore using the directory name specified
    *
    * @param constructorArguments constructorArguments(0) must specify the directory to which snapshots will be stored
    * @return the concrete FileStore to use
    */
  override def build(constructorArguments: Array[String]): SnapshotStore = {
    new FileSnapshotStore(constructorArguments(0))
  }

  /**
    * Writes a string to the persistent store
    *
    * @param instant date/time of the write, used to create the name, which will later be used in read() and purge()
    * @param content String to write
    * @return the Path of the file to which the String was written; @see java.nio.file.Path
    */
  override def write(instant: Instant,
                     content: String): AnyRef = {
    if (!Files.exists(directory)) {
      Files.createDirectories(directory)
    }
    val path = directory.resolve(createIso8601FileName(instant))
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  /**
    * Reads content from the persistent store
    *
    * @param instant date/time of the read
    * @return the content of the youngest item whose ISO-8601-based name is earlier or equal to instant
    */
  override def read(instant: Instant): Option[String] = {
    var optionString: Option[String] = None
    val filesWalk = Files.walk(directory, 1)
    try {
      val fileName = createIso8601FileName(instant)
      val fileToUse: Optional[Path] = filesWalk.filter(_.toFile.getName <= fileName).max(pathNameComparator)
      if (fileToUse.isPresent) {
        optionString = Some(Files.readAllLines(fileToUse.get).toArray.mkString("\n"))
      }
    } finally {
      filesWalk.close()
    }
    optionString
  }

  /**
    * Purges items from the persistent store
    *
    * @param instant date/time of items to be purged; items whose ISO-8601-based name is earlier than or equal to
    *                instant will be purged
    * @return the number of items purged
    */
  override def purge(instant: Instant): Integer = {
    val fileNameForInstant = createIso8601FileName(instant)
    val pathsToPurge: Array[AnyRef] = Files
      .walk(directory, 1)
      .filter(_.toFile.getName <= fileNameForInstant)
      .toArray
    for (anyRef <- pathsToPurge) {
      Files.delete(anyRef.asInstanceOf[Path])
    }
    pathsToPurge.length
  }

}
