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
    val path = "/mnt/pmem0/spark"
    val initializeSize = 1024L * 1024 * 1024
    val success = VMEMCacheJNI.initialize(path, initializeSize)


    val key = "key"

//    val vMemCacheManager = new OffHeapVmemCacheMemoryManager(SparkEnv.get)
//    val memoryBlockHolderPut = vMemCacheManager.allocate(108)

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
}
