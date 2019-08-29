/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.test;

import org.apache.spark.unsafe.NativeLoader;
import org.apache.spark.unsafe.VMEMCacheJNI;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestVMEMCacheJNI {

  @Before
  public void before() {
    NativeLoader.loadLibrary(VMEMCacheJNI.LIBRARY_NAME);
    try {
      String path = "/mnt/pmem0/spark";
      long initializeSize = 1024L * 462 * 1024 * 1024;
      int success = VMEMCacheJNI.initialize(path, initializeSize);
      assertTrue("Call VMEMCacheJNI.initialize error !!!", success == 0);
    } catch (Exception ex) {
      fail("Call VMEMCacheJNI.initialize exception error !!!");
    }
  }

  @Test
  public void testVMEMCacheJNI() {
    try {
      String key = "key";
      String valuePut = "value";
      int put = VMEMCacheJNI.put(key.getBytes(), null, 0, key.length(),
          valuePut.getBytes(), null, 0, valuePut.length());
      assertTrue("Test VMEMCacheJNI.put error !!!", put == 0);
      byte[] value = new byte[valuePut.length()];
      int get = VMEMCacheJNI.get(key.getBytes(), null, 0, key.length(),
          value, null, 0, value.length);
      assertTrue("Test VMEMCacheJNI.get error !!!", get == valuePut.length());
      String valueGet = new String(value);
      assertTrue("Test VMEMCacheJNI.get error, content not match !!!",
              valueGet.equals(valuePut));
      VMEMCacheJNI.evict(key.getBytes(), null, 0, key.length());
      get = VMEMCacheJNI.get(key.getBytes(), null, 0, key.length(),
          value, null, 0, value.length);
      assertTrue("Test VMEMCacheJNI.get error !!!", get == 0);
    } catch (Exception ex) {
      fail("TestVMEMCacheJNI.put exception error !!!");
    }
  }

  @Test
  public void testVMEMCacheJNIBenchmark() {
    try {
      long startTime, endTime;
      int minKeySize = 128;
      int maxKeySize = 256;
      int minValueSize = 7 * 1024 * 1024;
      int maxValueSize = 9 * 1024 * 1024;
      int numKeyValues = 640000;
      List<byte[]> keyArrays = new ArrayList<>(numKeyValues);
      List<ByteBuffer> valuePutBuffers = new ArrayList<>(numKeyValues);
      List<ByteBuffer> valueGetBuffers = new ArrayList<>(numKeyValues);

      for (int i = 0; i < numKeyValues; i += 1) {
        int randomValueSize = ThreadLocalRandom.current().nextInt(minValueSize, maxValueSize + 1);
        ByteBuffer valuePutBuffer = ByteBuffer.allocateDirect(randomValueSize);
        valuePutBuffer.put(getRandomBytes(randomValueSize));
        valuePutBuffer.flip();
        valuePutBuffers.add(valuePutBuffer);
        ByteBuffer valueGetBuffer = ByteBuffer.allocateDirect(maxValueSize);
        valueGetBuffers.add(valueGetBuffer);
        int randomKeySize = ThreadLocalRandom.current().nextInt(minKeySize, maxKeySize + 1);
        byte [] keyArray = getRandomBytes(randomKeySize);
        keyArrays.add(keyArray);
      }

      startTime = System.currentTimeMillis();
      for (int i = 0; i < numKeyValues; i += 1) {
        byte [] key = keyArrays.get(i);
        ByteBuffer value = valuePutBuffers.get(i);
        int put = VMEMCacheJNI.put(key, null, 0, key.length,
            null, value, 0, value.remaining());
        assertTrue("Test VMEMCacheJNI.put error !!!", put == 0);
      }
      endTime = System.currentTimeMillis();
      System.out.println("put " + numKeyValues + " key/value took "
              + (endTime - startTime) + " milliseconds in java");

      startTime = System.currentTimeMillis();
      for (int i = 0; i < numKeyValues; i += 1) {
        byte [] key = keyArrays.get(i);
        ByteBuffer valueGetBuffer = valueGetBuffers.get(i);
        int get = VMEMCacheJNI.get(key, null, 0, key.length,
            null, valueGetBuffer, 0, valueGetBuffer.remaining());
        assertTrue("Test VMEMCacheJNI.get error !!!", get > 0);
      }
      endTime = System.currentTimeMillis();
      System.out.println("get " + numKeyValues + " key/value took "
              + (endTime - startTime) + " milliseconds in java");

      for (int i = 0; i < numKeyValues; i += 1) {
        ByteBuffer valuePutBuffer = valuePutBuffers.get(i);
        ByteBuffer valueGetBuffer = valueGetBuffers.get(i);
        byte [] valueGet = new byte[valuePutBuffer.remaining()];
        byte [] valuePut = new byte[valuePutBuffer.remaining()];
        valueGetBuffer.get(valueGet);
        valuePutBuffer.get(valuePut);
        boolean equal = Arrays.equals(valueGet, valuePut);
        assertTrue("Test VMEMCacheJNI.get compare error,  !!!", equal == true);
      }
    } catch (Exception ex) {
      fail("TestVMEMcacheJNIBenchmark exception error !!!");
    }
  }

  static byte[] getRandomBytes(int size) {
    byte[] array = (byte[]) Array.newInstance(byte.class, size);
    for (int i = 0; i < size; i++) {
      array[i] = (byte) ThreadLocalRandom.current().nextInt(0, 256);
    }
    return array;
  }
}
