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
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.util.{Comparator, Optional}

class FileStore(val directoryName: String) extends StringStore {
  private val directory = Paths.get(directoryName)
  private val pathNameComparator = new Comparator[Path] {
    override def compare(o1: Path, o2: Path): Int = o1.toString.compareTo(o2.toString)
  }

  override def write(instant: Instant, content: String): Path = {
    if (!Files.exists(directory)) {
      Files.createDirectories(directory)
    }
    val path = directory.resolve(ISO_INSTANT.format(instant))
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  override def read(instant: Instant): Option[String] = {
    var optionString: Option[String] = None
    // No need to call filesWalk.close() because FileTreeWalker.close() method does not close any resources. But
    // https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#walk-java.nio.file.Path-java.nio.file.FileVisitOption...-
    // says "If timely disposal of file system resources is required, the try-with-resources construct should be used
    // to ensure that the stream's close method is invoked after the stream operations are completed," so this method
    // has a finally block to do so.
    val filesWalk = Files.walk(directory, 1)
    try {
      val fileName = ISO_INSTANT.format(instant)
      val fileToUse: Optional[Path] = filesWalk.filter(_.toFile.getName <= fileName).max(pathNameComparator)
      if (fileToUse.isPresent) {
        optionString = Some(Files.readAllLines(fileToUse.get).toArray.mkString("\n"))
      }
    } finally {
      filesWalk.close()
    }
    optionString
  }

  override def purge(instant: Instant): Integer = {
    val fileNameForInstant = ISO_INSTANT.format(instant)
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
