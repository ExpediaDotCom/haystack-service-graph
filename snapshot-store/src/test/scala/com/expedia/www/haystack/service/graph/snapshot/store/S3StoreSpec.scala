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

import java.time.Instant
import java.time.format.DateTimeFormatter.ISO_INSTANT

import collection.JavaConverters._
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder.standard
import com.amazonaws.services.s3.model.{ListObjectsV2Result, S3ObjectSummary}
import com.amazonaws.services.s3.AmazonS3
import com.expedia.www.haystack.service.graph.snapshot.store.S3StoreSpec.itemNamesWrittenToS3
import org.mockito.{Matchers, Mockito}
import org.mockito.Mockito.{times, verify, verifyNoMoreInteractions, when}
import org.mockito.Matchers.anyString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.collection.mutable

object S3StoreSpec {
  private val itemNamesWrittenToS3 = mutable.SortedSet[String]()
}

class S3StoreSpec extends StringStoreSpecBase with BeforeAndAfterAll with MockitoSugar {
  // Set to true to run these test in an integration-type way, talking to a real S3.
  // You must have valid keys on your machine to do so, typically in ~/.aws/credentials.
  private val useRealS3 = false

  private val bucketName = "haystack-service-graph-snapshots"
  private val folderName = "unit-test-snapshots"
  private val nextContinuationToken = "nextContinuationToken"
  private val listObjectsV2Result = mock[ListObjectsV2Result]
  private val s3Client = if (useRealS3) standard.withRegion(Regions.US_WEST_2).build else mock[AmazonS3]

  override def afterAll() {
    if (useRealS3) {
      itemNamesWrittenToS3.foreach(itemName => s3Client.deleteObject(bucketName, itemName))
      s3Client.deleteBucket(bucketName)
    }
    else {
      verifyNoMoreInteractionsForAllMocksThenReset()
    }
  }

  def convertStringToS3ObjectSummary(key: String): S3ObjectSummary = {
    val s3ObjectSummary = new S3ObjectSummary()
    s3ObjectSummary.setKey(key)
    s3ObjectSummary
  }

  describe("S3Store.build()") {
    val s3Store = new S3Store().build(Array(bucketName, folderName, "42")).asInstanceOf[S3Store]
    it("should use the arguments in the default constructor and the array") {
      val s3Client: AmazonS3 = s3Store.s3Client
      s3Client.getRegion.toString shouldEqual Regions.US_WEST_2.getName
      s3Store.bucketName shouldEqual bucketName
      s3Store.folderName shouldEqual folderName
      s3Store.listObjectsBatchSize shouldEqual 42
    }
  }

  describe("S3Store") {
    var s3Store = new S3Store(s3Client, bucketName, folderName, 3)
    it("should create the bucket when the bucket does not exist") {
      if(!useRealS3) {
        whensForWrite(false)
      }
      itemNamesWrittenToS3 += s3Store.write(oneMillisecondBeforeNow, oneMillisecondBeforeNowContent).asInstanceOf[String]
      if (!useRealS3) {
        verify(s3Client).doesBucketExistV2(bucketName)
        verify(s3Client).createBucket(bucketName)
        verify(s3Client).putObject(bucketName, createItemName(oneMillisecondBeforeNow), oneMillisecondBeforeNowContent)
        verifyNoMoreInteractionsForAllMocksThenReset()
      }
    }
    it("should not create the bucket when the bucket already exists") {
      if (!useRealS3) {
        whensForWrite(true)
      }
      itemNamesWrittenToS3 += s3Store.write(oneMillisecondAfterNow, oneMillisecondAfterNowContent).asInstanceOf[String]
      itemNamesWrittenToS3 += s3Store.write(twoMillisecondsAfterNow, twoMillisecondsAfterNowContent).asInstanceOf[String]
      if (!useRealS3) {
        verify(s3Client, times(2)).doesBucketExistV2(bucketName)
        verify(s3Client).putObject(bucketName, createItemName(oneMillisecondAfterNow), oneMillisecondAfterNowContent)
        verify(s3Client).putObject(bucketName, createItemName(twoMillisecondsAfterNow), twoMillisecondsAfterNowContent)
        verifyNoMoreInteractionsForAllMocksThenReset()
      }
    }
    it("should return None when read() is called with a time that is too early") {
      if (!useRealS3) {
        whensForRead
        when(listObjectsV2Result.isTruncated).thenReturn(false)
        val objectSummaries = itemNamesWrittenToS3.map(convertStringToS3ObjectSummary).toList.asJava
        when(listObjectsV2Result.getObjectSummaries).thenReturn(objectSummaries)
      }
      assert(s3Store.read(twoMillisecondsBeforeNow).isEmpty)
      if (!useRealS3) {
        verifiesForRead(1)
        verifyNoMoreInteractionsForAllMocksThenReset()
      }
    }
    it("should return the correct object when read() is called with a time that is not an exact match but is not too early") {
      if (!useRealS3) {
        whensForRead
        when(s3Client.getObjectAsString(anyString(), anyString())).thenReturn(oneMillisecondBeforeNowContent)
        when(listObjectsV2Result.isTruncated).thenReturn(false)
        val objectSummaries = itemNamesWrittenToS3.map(convertStringToS3ObjectSummary).toList.asJava
        when(listObjectsV2Result.getObjectSummaries).thenReturn(objectSummaries)
      }
      assert(s3Store.read(now).get == oneMillisecondBeforeNowContent)
      if (!useRealS3) {
        verifiesForRead(1)
        verify(s3Client).getObjectAsString(anyString(), Matchers.eq(createItemName(oneMillisecondBeforeNow)))
        verifyNoMoreInteractionsForAllMocksThenReset()
      }
    }
    it("should return the correct object when read() is called with a time that is an exact match") {
      if (!useRealS3) {
        whensForRead
        when(s3Client.getObjectAsString(anyString(), anyString())).thenReturn(twoMillisecondsAfterNowContent)
        when(listObjectsV2Result.isTruncated).thenReturn(false)
        val objectSummaries = itemNamesWrittenToS3.map(convertStringToS3ObjectSummary).toList.asJava
        when(listObjectsV2Result.getObjectSummaries).thenReturn(objectSummaries)
      }
      assert(s3Store.read(twoMillisecondsAfterNow).get == twoMillisecondsAfterNowContent)
      if (!useRealS3) {
        verifiesForRead(1)
        verify(s3Client).getObjectAsString(anyString(), Matchers.eq(createItemName(twoMillisecondsAfterNow)))
        verifyNoMoreInteractionsForAllMocksThenReset()
      }
    }
    it("should return the correct object for small batches") {
      s3Store = new S3Store(s3Client, bucketName, folderName, 1)
      if (!useRealS3) {
        whensForRead
        when(s3Client.getObjectAsString(anyString(), anyString())).thenReturn(twoMillisecondsAfterNowContent)
        when(listObjectsV2Result.isTruncated).thenReturn(true, true, false)
        val it = itemNamesWrittenToS3.iterator
        when(listObjectsV2Result.getObjectSummaries)
          .thenReturn(List(convertStringToS3ObjectSummary(it.next())).asJava)
          .thenReturn(List(convertStringToS3ObjectSummary(it.next())).asJava)
          .thenReturn(List(convertStringToS3ObjectSummary(it.next())).asJava)
      }
      assert(s3Store.read(twoMillisecondsAfterNow).get == twoMillisecondsAfterNowContent)
      if (!useRealS3) {
        verifiesForRead(3)
        verify(s3Client).getObjectAsString(anyString(), Matchers.eq(createItemName(twoMillisecondsAfterNow)))
        verifyNoMoreInteractionsForAllMocksThenReset()
      }
    }
    it("should never delete any items when purge() is called") {
      s3Store.purge(twoMillisecondsAfterNow) shouldEqual 0
      verifyNoMoreInteractionsForAllMocksThenReset()
    }
  }

  private def verifiesForRead(loopTimes: Int) = {
    verify(s3Client, times(loopTimes)).listObjectsV2(bucketName)
    verify(listObjectsV2Result, times(loopTimes)).getObjectSummaries
    verify(listObjectsV2Result, times(loopTimes)).getNextContinuationToken
    verify(listObjectsV2Result, times(loopTimes)).isTruncated
  }

  private def whensForRead = {
    when(listObjectsV2Result.getNextContinuationToken).thenReturn(nextContinuationToken)
    when(s3Client.listObjectsV2(anyString())).thenReturn(listObjectsV2Result)
  }

  private def whensForWrite(doesBucketExist: Boolean) = {
    when(s3Client.doesBucketExistV2(anyString())).thenReturn(doesBucketExist)
    when(listObjectsV2Result.getNextContinuationToken).thenReturn(nextContinuationToken)
  }

  private def verifyNoMoreInteractionsForAllMocksThenReset(): Unit = {
    verifyNoMoreInteractions(s3Client, listObjectsV2Result)
    Mockito.reset(s3Client, listObjectsV2Result)
  }

  private def createItemName(thisInstant: Instant) = {
    folderName + "/" + ISO_INSTANT.format(thisInstant)
  }
}