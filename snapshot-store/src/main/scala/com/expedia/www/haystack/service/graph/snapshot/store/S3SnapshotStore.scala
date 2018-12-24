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

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}

import scala.collection.JavaConverters._
import scala.math.Ordering.String.max

/**
  * Object that stores snapshots in S3
  * @param s3Client client with which to communicate with S3
  * @param bucketName name of the bucket
  * @param folderName name of the "folder" in the bucket (becomes the prefix of the S3 item name)
  * @param listObjectsBatchSize number of results to return with each listObjectsV2 request to S3; smaller values
  *                             use less memory at the cost of more calls to S3. The best value would be the maximum
  *                             number of snapshots that will exist in S3 before being purged; for example, with a
  *                             one hour snapshot interval and a snapshot TTL of 1 year, 366 * 24 = 8784 would be a good
  *                             value (perhaps rounded to 10,000)
  */
class S3SnapshotStore(val s3Client: AmazonS3,
                      val bucketName: String,
                      val folderName: String,
                      val listObjectsBatchSize: Int) extends SnapshotStore {
  private val itemNamePrefix = folderName + "/"

  def this() = {
    this(AmazonS3ClientBuilder.standard.withRegion(Regions.US_WEST_2).build, "", "", 0)
  }

  /**
    * Builds an S3SnapshotStore implementation given arguments to pass to the constructor
    *
    * @param constructorArguments [0] must be a String that specifies the bucket
    *                             [1] must be a String that specifies the folder in the bucket
    *                             [2] must be a String that specifies the batch count when listing items in the bucket
    * @return the S3SnapshotStore to use
    */
  override def build(constructorArguments: Array[String]): SnapshotStore = {
    val bucketName = constructorArguments(0)
    val folderName = constructorArguments(1)
    val listObjectsBatchSize = constructorArguments(2).toInt
    new S3SnapshotStore(s3Client, bucketName, folderName, listObjectsBatchSize)
  }

  /**
    * Writes a string to the persistent store
    *
    * @param instant date/time of the write, used to create the name, which will later be used in read() and purge()
    * @param content String to write
    * @return the item name of the object written to S3 (does not include the bucket name)
    */
  override def write(instant: Instant,
                     content: String): AnyRef = {
    if(!s3Client.doesBucketExistV2(bucketName)) {
      s3Client.createBucket(bucketName)
    }
    val itemName = createItemName(createIso8601FileName(instant))
    s3Client.putObject(bucketName, itemName, content)
    itemName
  }

  /**
    * Reads content from the persistent store
    *
    * @param instant date/time of the read
    * @return the content of the youngest item whose ISO-8601-based name is earlier or equal to instant
    */
  override def read(instant: Instant): Option[String] = {
    var content: Option[String] = None
    val itemName = getItemNameOfYoungestItemBeforeInstant(instant)
    if(itemName.isDefined) {
      content = Some(s3Client.getObjectAsString(bucketName, itemName.get))
    }
    content
  }

  /**
    * Purges items from the persistent store
    *
    * @param instant date/time of items to be purged; items whose ISO-8601-based name is earlier than or equal to
    *                instant will be purged
    * @return the number of items purged
    */
  private def getItemNameOfYoungestItemBeforeInstant(instant: Instant): Option[String] = {
    var optionString: Option[String] = None
    val listObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(listObjectsBatchSize)
    val instantAsItemName = createItemName(createIso8601FileName(instant))
    var listObjectsV2Result: ListObjectsV2Result = null
    do {
      listObjectsV2Result = s3Client.listObjectsV2(bucketName)
      val objectSummaries = listObjectsV2Result.getObjectSummaries.asScala
        .filter(_.getKey.startsWith(itemNamePrefix)).filter(_.getKey <= instantAsItemName)
      val potentialMax = if (objectSummaries.nonEmpty) Some(objectSummaries.maxBy(_.getKey).getKey) else None
      (optionString, potentialMax) match {
        case (None, None) =>
          optionString = None
        case (None, Some(_)) =>
          optionString = potentialMax
        case (Some(_), None) =>
          // optionString stays unchanged
        case (Some(optionStringItemName), Some(potentialMaxItemName)) =>
          optionString = Some(max(optionStringItemName, potentialMaxItemName))
      }
      listObjectsV2Request.setContinuationToken(listObjectsV2Result.getNextContinuationToken)
    } while (listObjectsV2Result.isTruncated)
    optionString
  }

  private def createItemName(fileName: String) = {
    s"$folderName/$fileName"
  }

}
