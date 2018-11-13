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

import java.io.File
import java.nio.file.{Files, Path}

class FileStoreSpec extends StringStoreSpecBase {
  private val directory = Files.createTempDirectory("FileStoreSpec")
  directory.toFile.deleteOnExit()
  private val directoryName = directory.toFile.getCanonicalPath

  describe("FileStore") {
    {
      val fileStore = new FileStore(directoryName)
      it("should use an existing directory without trying to create it when writing") {
        val pathFromWrite = fileStore.write(now, json.format(now.toString)).asInstanceOf[Path]
        assert(pathFromWrite.toFile.getCanonicalPath.startsWith(directoryName))
        assert(pathFromWrite.toFile.getCanonicalPath.endsWith(fileStore.createIso8601FileName(now)))
        fileStore.write(oneMillisecondBeforeNow, json.format(oneMillisecondBeforeNow.toString))
        fileStore.write(twoMillisecondsAfterNow, json.format(twoMillisecondsAfterNow.toString))
      }
      it("should return None when read() is called with a time that is too early") {
        val fileContent = fileStore.read(twoMillisecondsBeforeNow)
        assert(fileContent === None)
      }
      it("should read the correct file when read() is called with a later time") {
        val fileContent = fileStore.read(oneMillisecondAfterNow)
        assert(fileContent.get == json.format(now.toString))
      }
      it("should purge a single file when calling purge() with the timestamp of the oldest file") {
        val numberOfFilesPurged = fileStore.purge(oneMillisecondBeforeNow)
        numberOfFilesPurged shouldEqual 1
      }
      it("should purge the two remaining files when calling purge() with the youngest timestamp") {
        val numberOfFilesPurged = fileStore.purge(twoMillisecondsAfterNow)
        numberOfFilesPurged shouldEqual 2
      }
    }
    it("should create the directory when the directory does not exist") {
      val suffix = File.separator + "DirectoryToCreate"
      val fileStore = new FileStore(directoryName + suffix)
      val pathFromWrite = fileStore.write(now, json.format(now.toString)).asInstanceOf[Path]
      assert(pathFromWrite.toFile.getCanonicalPath.startsWith(directoryName + suffix))
      val fileContent = fileStore.read(now)
      assert(fileContent.get == json.format(now.toString))
      val numberOfFilesPurged = fileStore.purge(now)
      numberOfFilesPurged shouldEqual 1
    }
  }
}
