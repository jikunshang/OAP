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
package org.apache.spark.unsafe;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI wrapper for the vmemcache library.
 */
public class VMEMCacheJNI {

    private static final Logger LOG = LoggerFactory.getLogger(VMEMCacheJNI.class);
    private static boolean initialized = false;
    public static final String LIBRARY_NAME = "vmemcachejni";

    static {
        LOG.info("Trying to load the native library from jni...");
        NativeLoader.loadLibrary(LIBRARY_NAME);
    }

    public static synchronized int initialize(String path, long maxSize) {
        if (!initialized) {
            int success = init(path, maxSize);
            if (success == 0) {
                initialized = true;
            }
            return success;
        }
        return 0;
    }

    static native int init(String path, long maxSize);

    /* returns the number of bytes put */
    public static native int put(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen,
                                 byte[] valueArray, ByteBuffer valueBuffer,
                                 int valueOff, int valueLen);

    /* returns the number of bytes get */
    public static native int get(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen,
                                 byte[] valueArray, ByteBuffer valueBuffer,
                                 int valueOff, int maxValueLen);

    public static native int evict(byte[] keyArray, ByteBuffer keyBuffer, int keyOff, int keyLen);
}
