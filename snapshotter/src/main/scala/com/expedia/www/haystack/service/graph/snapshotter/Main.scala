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
package com.expedia.www.haystack.service.graph.snapshotter

import com.expedia.www.haystack.service.graph.snapshot.store.StringStore
import org.slf4j.{Logger, LoggerFactory}

object Main {
  val StringStoreClassRequiredMsg: String = "The first argument must specify the fully qualified class name of a class that implements StringStore"
  var LOGGER: Logger = LoggerFactory.getLogger(Main.getClass)

  /**
    * Main method
    *
    * @param args specifies the class to run and its parameters
    *             args[0] is the fully qualified class name of the implementation of
    *             com.expedia.www.haystack.service.graph.snapshot.store.StringStore to run;
    *             args[1] and greater are the arguments to the constructor of the class specified by arg[0]; these
    *             values must be Scala code that will be interpreted to instantiate and run the desired class. See the
    *             documentation in the build() method of the desired implementation for details on the arguments.
    *             Examples:
    *             <ul>
    *               <li>To run FileStore and use /var/snapshots for snapshot storage, the arguments should be:
    *                 <ul>
    *                   <li>com.expedia.www.haystack.service.graph.snapshot.store.FileStore</li>
    *                   <li>"/var/snapshots"</li>
    *                 </ul>
    *               </li>
    *               <li>To run S3Store and use the "Haystack" bucket with subfolder "snapshots" for snapshot storage,
    *               the arguments would be something like:
    *                 <ul>
    *                   <li>com.expedia.www.haystack.service.graph.snapshot.store.S3Store</li>
    *                   <li>"haystack-bucket"</li>
    *                   <li>"snapshots-folder"</li>
    *                   <li>10000</li>
    *                 </ul>
    *               </li>
    *             </ul>
    */
  def main(args: Array[String]): Unit = {
    // Determine what StringStore to use (specified in configuration)
    if(args.length == 0) {
      LOGGER.error(StringStoreClassRequiredMsg)
    } else {
      val stringStore = instantiateStringStore(args)
      // Call the service-graph endpoint (specified in configuration) to get a service graph
      // Store the service graph in the StringStore
      // Write appropriate metrics
    }
  }

  private def instantiateStringStore(args: Array[String]): StringStore = {
    val fullyQualifiedClassName = args(0)
    val klass = Class.forName(fullyQualifiedClassName)
    val defaultConstructorInstance = klass.newInstance().asInstanceOf[StringStore]
    val stringStore = defaultConstructorInstance.build(args.drop(1))
    stringStore
  }
}
