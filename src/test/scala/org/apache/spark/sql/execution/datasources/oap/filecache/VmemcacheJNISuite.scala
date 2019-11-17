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

import java.nio.{ByteBuffer, ByteOrder}

import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.{Platform, VMEMCacheJNI}

class VmemcacheJNISuite extends SharedOapContext{


  test("test Native address ") {


    val key = "key"

    // copy length to offheap buffer
    val bbPut = ByteBuffer.allocateDirect(400)
    for(i <- 0 until 50) {
      bbPut.putLong(i.toLong)
    }
    bbPut.position(0)

    // put
    VMEMCacheJNI.putNative(key.getBytes, null, 0, key.length,
      bbPut.asInstanceOf[DirectBuffer].address(), 0, 400)

    // get
    val bbGet1 = ByteBuffer.allocateDirect(200)
    val bbGet2 = ByteBuffer.allocateDirect(200)
    // get 0- 200
    VMEMCacheJNI.getNative(key.getBytes, null, 0, key.length,
      bbGet1.asInstanceOf[DirectBuffer].address(), 0, 200)
    // get 200 - 400
    VMEMCacheJNI.getNative(key.getBytes, null, 0, key.length,
      bbGet2.asInstanceOf[DirectBuffer].address(), 200, 200)
//    for(i <- 0 until 20)
//      print(bbPut.get())
//    for(i <- 0 until 20)
//      print(bbGet1.get())

    bbPut.asLongBuffer()
    bbGet1.asLongBuffer()
    bbGet2.asLongBuffer()
    for ( i <- 0 until 25) {
//      assert( bbPut.asLongBuffer().get(i) == bbGet1.asLongBuffer().get(i))
      assert(bbPut.getLong() == bbGet1.getLong())
//      print(bbPut.get() + "\n")
    }

    for( i <- 0 until 25) {
      assert( bbPut.getLong() == bbGet2.getLong())
    }

  }

  test("platform test") {
    val src: Long = 1

    val dst = ByteBuffer.allocateDirect(100)
    val get = new Array[Byte](100)

    Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET,
      null, dst.asInstanceOf[DirectBuffer].address(), LongType.defaultSize)

    Platform.copyMemory(null, dst.asInstanceOf[DirectBuffer].address(),
      get, Platform.BYTE_ARRAY_OFFSET, 100)

    print(ByteBuffer.wrap(get).getLong())
  }

  test(s"vmemcache_get overhead") {
    val path = "/mnt/pmem0/spark"
    val initializeSize = 1024L * 1024 * 1024
    val success = VMEMCacheJNI.initialize(path, initializeSize)

    val SIZE_1M = 1024 * 1024
    val SIZE_10M = 10 * SIZE_1M
    val threadNum = 1

    val threadArray = new Array[Thread](threadNum)
    for( tid <- 0 until threadNum) {
      val t = new Thread(new testThread(tid))
      threadArray(tid) = t
      t.start()
    }

    threadArray.foreach(t => t.join())

    class testThread(id: Int) extends Runnable {
      override def run: Unit = {
        val key = "This is key " + id.toString
        val bbPut = ByteBuffer.allocateDirect(SIZE_10M)
        for (i <- 0 until bbPut.limit() / LongType.defaultSize) {
          bbPut.putLong(i.toLong)
        }
        var startTime = 0L
        VMEMCacheJNI.putNative(key.getBytes(), null, 0, key.getBytes().length,
          bbPut.asInstanceOf[DirectBuffer].address(), 0, SIZE_10M)

        // 2.get 1MB 10 times
        val bbGet1M = ByteBuffer.allocateDirect(SIZE_10M)
        var totalTime: Long = 0
        val timeArray = new Array[Long](10)
        for (i <- 0 until 10) {
          startTime = System.nanoTime()
          VMEMCacheJNI.getNative(key.getBytes(), null, 0, key.getBytes().length,
            bbGet1M.asInstanceOf[DirectBuffer].address() + i * SIZE_1M, 0, SIZE_1M)
          val endTime = System.nanoTime()
          totalTime += (endTime - startTime)
          timeArray(i) = (endTime - startTime)
        }
        print(s"ThreadId: ${id} vmemcache get 1M total takes ${totalTime / 1000} us.\n\n")

        // 1.get 10MB direct
        val bbGet10M = ByteBuffer.allocateDirect(SIZE_10M)
        startTime = System.nanoTime()
        VMEMCacheJNI.getNative(key.getBytes(), null, 0, key.getBytes().length,
        bbGet10M.asInstanceOf[DirectBuffer].address(), 0, SIZE_10M)
        print(s"threadId: ${id} vmemcache get 10M takes" +
        s" ${(System.nanoTime() - startTime) / 1000} us.\n\n")

      }
    }
  }
}
