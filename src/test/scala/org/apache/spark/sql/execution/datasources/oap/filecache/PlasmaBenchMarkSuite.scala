/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.oap.filecache

import java.nio.ByteBuffer

import org.apache.arrow.plasma
import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.unsafe.Platform

class PlasmaBenchMarkSuite extends SharedOapContext {

  test("external cache benchmark") {
    val threadNum = 10
    val objectsInThread = 1
    val objectIds = new Array[Array[Byte]](threadNum * objectsInThread)
    val threads = new Array[Thread](threadNum)
    val cache = new ExternalCache()
    val plasmaClient = new plasma.PlasmaClient("/tmp/plasmaStore", "", 0)
    val SIZE_10M = 10 * 1024 * 1024

    class PutThread(threadId: Int) extends Thread {
      override def run(): Unit = {
        val data = ByteBuffer.allocateDirect(SIZE_10M)
        val startTime = System.nanoTime()
        for (i <- 0 until objectsInThread) {
          val objId = threadId * objectsInThread + i
          objectIds(objId) = cache.hash(objId.toString)
          val bb: ByteBuffer = plasmaClient.create(objectIds(objId), SIZE_10M)
          Platform.copyMemory(null, data.asInstanceOf[DirectBuffer].address(),
            null, bb.asInstanceOf[DirectBuffer].address(), SIZE_10M)
          plasmaClient.seal( objectIds(objId) )
          plasmaClient.release( objectIds(objId) )
        }
        val duration = System.nanoTime() - startTime
        print(s"thread Id: $threadId put $objectsInThread objects, takes $duration ns." +
          s" throughput is ${objectsInThread * SIZE_10M / (duration.toDouble / 1000000000)} MB/s\n")
      }
    }

    class GetThread(threadId: Int) extends Thread {
      override def run(): Unit = {
        val data = ByteBuffer.allocateDirect(SIZE_10M)
        val startTime = System.nanoTime()
        for (i <- 0 until objectsInThread) {
          val objId = threadId * objectsInThread + i
          objectIds(objId) = cache.hash(objId.toString)
          val bb: ByteBuffer = plasmaClient.getByteBuffer(objectIds(objId), 2000, false)
          Platform.copyMemory(null, bb.asInstanceOf[DirectBuffer].address(),
            null, data.asInstanceOf[DirectBuffer].address(), SIZE_10M)
        }
        val duration = System.nanoTime() - startTime
        print(s"thread Id: $threadId get $objectsInThread objects, takes $duration ns." +
          s" throughput is ${(objectsInThread * 10 / (duration.toDouble / 1000000000)).toLong}" +
          s" MB/s\n")
      }
    }

    // start Put
    val putStartTime = System.nanoTime()
    for( i <- 0 until threadNum) {
      threads(i) = new PutThread(i)
      threads(i).run()
    }

    for( i <- 0 until threadNum) {
      threads(i).join()
    }
    val putDuraion = System.nanoTime() - putStartTime
    print(s"\ntotal put throughput is " +
      s"${threadNum * objectsInThread * SIZE_10M / (putDuraion.toDouble / 1000000000)} B/s.\n\n")

    // start Get
    val getStartTime = System.nanoTime()
    for( i <- 0 until threadNum) {
      threads(i) = new GetThread(i)
      threads(i).run()
    }

    for( i <- 0 until threadNum) {
      threads(i).join()
    }
    val getDuraion = System.nanoTime() - getStartTime
    print(s"\ntotal get throughput is " +
      s"${threadNum * objectsInThread * SIZE_10M / (getDuraion.toDouble / 1000000000)} B/s.\n\n")

  }

}
