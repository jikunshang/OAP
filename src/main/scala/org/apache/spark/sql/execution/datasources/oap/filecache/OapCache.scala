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
import java.util.concurrent.{ConcurrentHashMap, Executors, ExecutorService}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import com.google.common.cache._
import com.google.common.hash._
import org.apache.arrow.plasma
import sun.nio.ch.DirectBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.{Platform, VMEMCacheJNI}
import org.apache.spark.util.Utils

trait OapCache {
  val dataFiberSize: AtomicLong = new AtomicLong(0)
  val indexFiberSize: AtomicLong = new AtomicLong(0)
  val dataFiberCount: AtomicLong = new AtomicLong(0)
  val indexFiberCount: AtomicLong = new AtomicLong(0)

  def get(fiber: FiberId): FiberCache
  def put(fiber: FiberCache): Unit
  def getIfPresent(fiber: FiberId): FiberCache
  def getFibers: Set[FiberId]
  def invalidate(fiber: FiberId): Unit
  def invalidateAll(fibers: Iterable[FiberId]): Unit
  def cacheSize: Long
  def cacheCount: Long
  def cacheStats: CacheStats
  def pendingFiberCount: Int
  def cleanUp(): Unit = {
    invalidateAll(getFibers)
    dataFiberSize.set(0L)
    dataFiberCount.set(0L)
    indexFiberSize.set(0L)
    indexFiberCount.set(0L)
  }

  def incFiberCountAndSize(fiber: FiberId, count: Long, size: Long): Unit = {
    if (fiber.isInstanceOf[DataFiberId] || fiber.isInstanceOf[TestDataFiberId]) {
      dataFiberCount.addAndGet(count)
      dataFiberSize.addAndGet(size)
    } else if (
      fiber.isInstanceOf[BTreeFiberId] ||
      fiber.isInstanceOf[BitmapFiberId] ||
      fiber.isInstanceOf[TestIndexFiberId]) {
      indexFiberCount.addAndGet(count)
      indexFiberSize.addAndGet(size)
    }
  }

  def decFiberCountAndSize(fiber: FiberId, count: Long, size: Long): Unit =
    incFiberCountAndSize(fiber, -count, -size)

  protected def cache(fiber: FiberId): FiberCache = {
    val cache = fiber match {
      case DataFiberId(file, columnIndex, rowGroupId) => file.cache(rowGroupId, columnIndex, fiber)
      case BTreeFiberId(getFiberData, _, _, _) => getFiberData.apply()
      case BitmapFiberId(getFiberData, _, _, _) => getFiberData.apply()
      case TestDataFiberId(getFiberData, _) => getFiberData.apply()
      case TestIndexFiberId(getFiberData, _) => getFiberData.apply()
      case _ => throw new OapException("Unexpected FiberId type!")
    }
    cache.fiberId = fiber
    cache
  }

}

class NonEvictPMCache(dramSize: Long,
                      pmSize: Long,
                      cacheGuardianMemory: Long) extends OapCache with Logging {
  // We don't bother the memory use of Simple Cache
  private val cacheGuardian = new CacheGuardian(Int.MaxValue)
  private val _cacheSize: AtomicLong = new AtomicLong(0)
  private val _cacheCount: AtomicLong = new AtomicLong(0)

  val cacheMap : ConcurrentHashMap[FiberId, FiberCache] = new ConcurrentHashMap[FiberId, FiberCache]
  cacheGuardian.start()

  override def get(fiber: FiberId): FiberCache = {
    if (cacheMap.containsKey(fiber)) {
      val fiberCache = cacheMap.get(fiber)
      fiberCache.occupy()
      fiberCache
    } else {
      if (cacheSize < pmSize) {
        val fiberCache = cache(fiber)
        incFiberCountAndSize(fiber, 1, fiberCache.size())
        _cacheSize.addAndGet(fiberCache.size())
        _cacheCount.addAndGet(1)
        fiberCache.occupy()
        cacheMap.put(fiber, fiberCache)
        fiberCache
      } else {
        val fiberCache = cache(fiber)
        incFiberCountAndSize(fiber, 1, fiberCache.size())
        fiberCache.occupy()
        // We only use fiber for once, and CacheGuardian will dispose it after release.
        cacheGuardian.addRemovalFiber(fiber, fiberCache)
        decFiberCountAndSize(fiber, 1, fiberCache.size())
        fiberCache
      }
    }
  }

  override def put(fiber: FiberCache): Unit = {
    throw new OapException("Unsupported in NonEvict Cache")
  }

  override def getIfPresent(fiber: FiberId): FiberCache = {
    if (cacheMap.contains(fiber)) {
      cacheMap.get(fiber)
    } else {
      throw new RuntimeException("Key not found")
    }
  }

  override def getFibers: Set[FiberId] = {
    // throw new RuntimeException("Unsupported")
    cacheMap.keySet().asScala.toSet
  }

  override def invalidate(fiber: FiberId): Unit = {
    if (cacheMap.contains(fiber)) {
      cacheMap.remove(fiber)
    }
  }

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {
    fibers.foreach(invalidate)
  }

  override def cacheSize: Long = {_cacheSize.get()}

  override def cacheCount: Long = {_cacheCount.get()}

  override def cacheStats: CacheStats = CacheStats()

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount
}

class SimpleOapCache extends OapCache with Logging {

  // We don't bother the memory use of Simple Cache
  private val cacheGuardian = new CacheGuardian(Int.MaxValue)
  cacheGuardian.start()

  override def get(fiberId: FiberId): FiberCache = {
    val fiberCache = cache(fiberId)
    incFiberCountAndSize(fiberId, 1, fiberCache.size())
    fiberCache.occupy()
    // We only use fiber for once, and CacheGuardian will dispose it after release.
    cacheGuardian.addRemovalFiber(fiberId, fiberCache)
    decFiberCountAndSize(fiberId, 1, fiberCache.size())
    fiberCache
  }

  override def put(fiber: FiberCache): Unit = {
    throw new OapException("Unsupported in SimpleCache")
  }
  override def getIfPresent(fiber: FiberId): FiberCache = null

  override def getFibers: Set[FiberId] = {
    Set.empty
  }

  override def invalidate(fiber: FiberId): Unit = {}

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {}

  override def cacheSize: Long = 0

  override def cacheStats: CacheStats = CacheStats()

  override def cacheCount: Long = 0

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount
}

class VMemCache extends OapCache with Logging {
  private def emptyDataFiber(fiberLength: Long): FiberCache =
    OapRuntime.getOrCreate.memoryManager.getEmptyDataFiberCache(fiberLength)
  private val cacheHitCount: AtomicLong = new AtomicLong(0)
  private val cacheMissCount: AtomicLong = new AtomicLong(0)
  private val cacheTotalGetTime: AtomicLong = new AtomicLong(0)
  private var cacheTotalCount: Long = 0
  private var cacheEvictCount: Long = 0
  private var cacheTotalSize: Long = 0
  private var topOffheapUsed: Long = 0
  // We don't bother the memory use of Simple Cache
  private val cacheGuardian = new CacheGuardian(Int.MaxValue)
  cacheGuardian.start()

  val conf = SparkEnv.get.conf
  val isAsyncWrite = conf.getBoolean(OapConf.OAP_FIBERCACHE_ASYNC_WRITE.key,
    OapConf.OAP_FIBERCACHE_ASYNC_WRITE.defaultValue.get)
  val asyncWriteThreadNum = conf.getInt(OapConf.OAP_FIBERCACHE_ASYNC_WRITE_THREAD_NUM.key,
    OapConf.OAP_FIBERCACHE_ASYNC_WRITE_THREAD_NUM.defaultValue.get)
  val asyncWriteBatch = conf.getLong(OapConf.OAP_FIBERCACHE_ASYNC_WRITE_BATCH_SIZE.key,
    OapConf.OAP_FIBERCACHE_ASYNC_WRITE_BATCH_SIZE.defaultValue.get)
  logInfo(s"Async get Fibers is $isAsyncWrite, thread num is $asyncWriteThreadNum," +
    s" batch size is $asyncWriteBatch")
  lazy val threadPool: ExecutorService = Executors.newFixedThreadPool(24)

  var fiberSet = scala.collection.mutable.Set[FiberId]()
  override def get(fiber: FiberId): FiberCache = {
    val fiberKey = fiber.toFiberKey()
    val startTime = System.currentTimeMillis()
    val res = VMEMCacheJNI.exist(fiberKey.getBytes(), null, 0, fiberKey.getBytes().length)
    logDebug(s"vmemcache.exist return $res ," +
      s" takes ${System.currentTimeMillis() - startTime} ms")
    if (res <= 0) {
      cacheMissCount.addAndGet(1)
      val fiberCache = cache(fiber)
      fiberSet.add(fiber)
      fiberCache.occupy()
      cacheGuardian.addRemovalFiber(fiber, fiberCache)
      fiberCache
    } else { // cache hit
      cacheHitCount.addAndGet(1)
      val length = res
      val fiberCache = emptyDataFiber(length)

      // if needs params, will be fiberCache, fiberId, dataLength
      class asyncGetThread extends Runnable {
        override def run(): Unit = {
          val startTime = System.nanoTime()
          val batchSize = asyncWriteBatch
          val batches: Long = length / batchSize
          val batchLeft: Long = length % batchSize
          for (i: Long <- 0L until batches) {
            VMEMCacheJNI.getNative(fiberKey.getBytes(), null,
              0, fiberKey.getBytes().length, fiberCache.getBaseOffset + i * batchSize,
              (i * batchSize).toInt, batchSize.toInt)
            fiberCache.asyncWriteOffset(batchSize)
          }
          VMEMCacheJNI.getNative(fiberKey.getBytes(), null,
            0, fiberKey.getBytes().length, fiberCache.getBaseOffset + batches * batchSize,
             (batches * batchSize).toInt, batchLeft.toInt)
          fiberCache.asyncWriteOffset(batchLeft)
          val duration = (System.nanoTime() - startTime)
          logDebug(s"async getNative total takes ${duration} ns")
          cacheTotalGetTime.addAndGet(duration)
        }
      }
      if(isAsyncWrite) {
        threadPool.execute(new asyncGetThread())
      } else {
        val startTime = System.nanoTime()
        val get = VMEMCacheJNI.getNative(fiberKey.getBytes(), null, 0, fiberKey.getBytes().length,
          fiberCache.getBaseOffset, 0, fiberCache.size().toInt)
        val duration = (System.nanoTime() - startTime)
        logDebug(s"getNative require ${length} bytes. " +
          s"returns $get bytes, takes ${duration} ns")
        cacheTotalGetTime.addAndGet(duration)
        fiberCache.asyncWriteOffset(fiberCache.size())
      }
      fiberCache.fiberId = fiber
      fiberCache.occupy()
      cacheGuardian.addRemovalFiber(fiber, fiberCache)
      fiberCache
    }
  }

  def put(fiber: FiberCache): Unit = {
    fiber.asyncWriteOffset(fiber.size())
    val fiberId = fiber.fiberId
    logDebug(s"vmemcacheput params: fiberkey size: ${fiberId.toFiberKey().length}," +
      s"addr: ${fiber.getBaseOffset}, length: ${fiber.getOccupiedSize().toInt} ")
    val startTime = System.currentTimeMillis()
    val put = VMEMCacheJNI.putNative(fiberId.toFiberKey().getBytes(), null, 0,
      fiberId.toFiberKey().length, fiber.getBaseOffset,
      0, fiber.getOccupiedSize().toInt)
    logDebug(s"Vmemcache_put returns $put ," +
      s"takes ${System.currentTimeMillis() - startTime} ms ")
  }

  override def getIfPresent(fiber: FiberId): FiberCache = null

  override def getFibers: Set[FiberId] = {
    val tmpFiberSet = fiberSet
    // todo: we can implement a VmemcacheJNI.exist(keys:byte[][])
    for(fibId <- tmpFiberSet) {
      val fiberKey = fibId.toFiberKey()
      val get = VMEMCacheJNI.exist(fiberKey.getBytes(), null, 0, fiberKey.getBytes().length)
      if(get <=0 ) {
        fiberSet.remove(fibId)
        logDebug(s"$fiberKey is removed.")
      } else {
        logDebug(s"$fiberKey is still stored.")
      }
    }
    // logInfo(fiberSet.toString());
    fiberSet.toSet
  }

  override def invalidate(fiber: FiberId): Unit = {}

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {}

  override def cacheSize: Long = 0

  override def cacheStats: CacheStats = {
    val status = new Array[Long](3)
    VMEMCacheJNI.status(status)
    cacheEvictCount = status(0)
    cacheTotalCount = status(1)
    cacheTotalSize = status(2)
    logDebug(s"Current status is evict:$cacheEvictCount," +
      s" count:$cacheTotalCount, size:$cacheTotalSize")
    CacheStats(
      // fiberSet.size, // dataFiberCount
      cacheTotalCount, // dataFiberCount
      cacheTotalSize, // dataFiberSize JNIGet
      0, // indexFiberCount
      0, // indexFiberSize
      cacheGuardian.pendingFiberCount, // pendingFiberCount
      cacheGuardian.pendingFiberSize, // pendingFiberSize
      cacheHitCount.get(), // dataFiberHitCount
      cacheMissCount.get(), // dataFiberMissCount
      cacheHitCount.get(), // dataFiberLoadCount
      cacheTotalGetTime.get(), // dataTotalLoadTime
      cacheEvictCount, // dataEvictionCount
      0, // indexFiberHitCount
      0, // indexFiberMissCount
      0, // indexFiberLoadCount
      0, // indexFiberLoadTime
      0) // indexEvictionCount JNIGet
  }

  override def cacheCount: Long = 0

  override def pendingFiberCount: Int = 0

  override def cleanUp: Unit = {
    super.cleanUp
  }
}

class ExternalCache extends OapCache with Logging {
  private def emptyDataFiber(fiberLength: Long): FiberCache =
    OapRuntime.getOrCreate.memoryManager.getEmptyDataFiberCache(fiberLength)
  // for UT
  private def emptyDataFiberByteBuffer(fiberLength: Long) : FiberCache = {
    val data = ByteBuffer.allocateDirect(fiberLength.toInt)
    val mem = MemoryBlockHolder(CacheEnum.GENERAL, null, data.asInstanceOf[DirectBuffer].address(),
      fiberLength, fiberLength, "DRAM")
    FiberCache(mem)
  }
  private val conf = SparkEnv.get.conf
  private val externalStoreCacheSocket: String =
    conf.get(OapConf.OAP_FIBERCACHE_EXTERNAL_STORE_PATH)
  val isAsyncWrite = conf.getBoolean(OapConf.OAP_FIBERCACHE_ASYNC_WRITE.key,
    OapConf.OAP_FIBERCACHE_ASYNC_WRITE.defaultValue.get)
  val asyncWriteThreadNum = conf.getInt(OapConf.OAP_FIBERCACHE_ASYNC_WRITE_THREAD_NUM.key,
    OapConf.OAP_FIBERCACHE_ASYNC_WRITE_THREAD_NUM.defaultValue.get)
  val asyncWriteBatch = conf.getLong(OapConf.OAP_FIBERCACHE_ASYNC_WRITE_BATCH_SIZE.key,
    OapConf.OAP_FIBERCACHE_ASYNC_WRITE_BATCH_SIZE.defaultValue.get)
  logInfo(s"Async get Fibers is $isAsyncWrite, thread num is $asyncWriteThreadNum," +
    s" batch size is $asyncWriteBatch")
  lazy val threadPool: ExecutorService = Executors.newFixedThreadPool(24)

  private var cacheInit: Boolean = false
  def init(): Unit = {
    if (!cacheInit) {
      try {
        System.loadLibrary("plasma_java")
      } catch {
        case e: Exception => logError(s"load plasma jni lib failed " + e.getMessage)
      }
      cacheInit = true
    }
  }
  init()

  private val cacheHitCount: AtomicLong = new AtomicLong(0)
  private val cacheMissCount: AtomicLong = new AtomicLong(0)
  private val cacheTotalGetTime: AtomicLong = new AtomicLong(0)
  private var cacheTotalCount: Long = 0
  private var cacheEvictCount: Long = 0
  private var cacheTotalSize: Long = 0

  private val cacheGuardian = new CacheGuardian(Int.MaxValue)
  cacheGuardian.start()
  val plasmaClient = new plasma.PlasmaClient(externalStoreCacheSocket, "", 0)
  val hf: HashFunction = Hashing.murmur3_128()

  def hash(key: Array[Byte]): Array[Byte] = {
    val ret = new Array[Byte](20)
    hf.newHasher().putBytes(key).hash().writeBytesTo(ret, 0, 20)
    ret
  }
  def hash(key: String): Array[Byte] = {
    hash(key.getBytes())
  }

  def delete(fiberId: FiberId): Unit = {
    val objectId = hash(fiberId.toString)
    plasmaClient.delete(objectId)
  }

  def contains(fiberId: FiberId): Boolean = {
    val objectId = hash(fiberId.toString)
    if (plasmaClient.contains(objectId)) true
    else false
  }

  override def get(fiberId: FiberId): FiberCache = {
    val objectId = hash(fiberId.toString)
    if(contains(fiberId)) {
      logDebug(s"Cache hit, get from external cache.")
      cacheHitCount.addAndGet(1)
      val bb: ByteBuffer = plasmaClient.getByteBuffer(objectId, 2000, false)
      val length = bb.capacity()
      val fiberCache = emptyDataFiber(length)
      fiberCache.fiberId = fiberId
      if(isAsyncWrite) {
        class asyncGetThread extends Runnable {
          override def run(): Unit = {
            val startTime = System.nanoTime()
            val batchSize = asyncWriteBatch
            val batches: Long = length / batchSize
            val batchLeft: Long = length % batchSize
            for (i: Long <- 0L until batches) {
              Platform.copyMemory(null, bb.asInstanceOf[DirectBuffer].address() + i * batchSize,
                null, fiberCache.getBaseOffset + i * batchSize, batchSize)
              fiberCache.asyncWriteOffset(batchSize)
            }
            Platform.copyMemory(null, bb.asInstanceOf[DirectBuffer].address() + batches * batchSize,
              null, fiberCache.getBaseOffset + batches * batchSize, batchLeft)
            fiberCache.asyncWriteOffset(batchLeft)
            val duration = (System.nanoTime() - startTime)
            logDebug(s"async getNative total takes ${duration} ns")
            cacheTotalGetTime.addAndGet(duration)
          }
        }
        threadPool.execute(new asyncGetThread())
      } else {
        val startTime = System.nanoTime()
        Platform.copyMemory(null, bb.asInstanceOf[DirectBuffer].address(),
          null, fiberCache.getBaseOffset, bb.capacity())
        val duration = System.nanoTime() - startTime
        logDebug(s"copy from ByteBuffer takes ${duration} ms")
        cacheTotalGetTime.addAndGet(duration)
      }
      fiberCache.occupy()
      cacheGuardian.addRemovalFiber(fiberId, fiberCache)
      fiberCache
    } else {
      cacheMissCount.addAndGet(1)
      val fiberCache = cache(fiberId)
      fiberCache.occupy()
      cacheGuardian.addRemovalFiber(fiberId, fiberCache)
      fiberCache
    }
  }

  // todo: can be async here
  override def put(fiber: FiberCache) {
    fiber.occupy()
    val fiberId = fiber.fiberId
    val objectId = hash(fiberId.toString)
    if( !contains(fiberId)) {
      logDebug(s"Cache miss, put into external cache")
      val bb: ByteBuffer = plasmaClient.create(objectId, fiber.size().toInt)
      Platform.copyMemory(null, fiber.getBaseOffset,
        null, bb.asInstanceOf[DirectBuffer].address(), fiber.size())
      plasmaClient.seal(objectId)
      plasmaClient.release(objectId)
    }
    fiber.release()
  }
  override def getIfPresent(fiber: FiberId): FiberCache = null

  override def getFibers: Set[FiberId] = {
    Set.empty
  }

  override def invalidate(fiber: FiberId): Unit = { }

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = { }

  override def cacheSize: Long = 0

  override def cacheCount: Long = 0

  override def cacheStats: CacheStats = {
    CacheStats(0,
      0, 0, 0,
      cacheGuardian.pendingFiberCount,
      cacheGuardian.pendingFiberSize,
      cacheHitCount.get(),
      cacheMissCount.get(),
      0, 0, 0,
      0, 0, 0, 0, 0 // index fiberCache
    )
  }

  override def pendingFiberCount: Int = 0


}

class GuavaOapCache(
    dataCacheMemory: Long,
    indexCacheMemory: Long,
    cacheGuardianMemory: Long,
    var indexDataSeparationEnable: Boolean)
    extends OapCache with Logging {

  // TODO: CacheGuardian can also track cache statistics periodically
  private val cacheGuardian = new CacheGuardian(cacheGuardianMemory)
  cacheGuardian.start()

  private val KB: Double = 1024
  private val DATA_MAX_WEIGHT = (dataCacheMemory / KB).toInt
  private val INDEX_MAX_WEIGHT = (indexCacheMemory / KB).toInt
  private val TOTAL_MAX_WEIGHT = INDEX_MAX_WEIGHT + DATA_MAX_WEIGHT
  private val CONCURRENCY_LEVEL = 4

  // Total cached size for debug purpose, not include pending fiber
  private val _cacheSize: AtomicLong = new AtomicLong(0)

  private val removalListener = new RemovalListener[FiberId, FiberCache] {
    override def onRemoval(notification: RemovalNotification[FiberId, FiberCache]): Unit = {
      logDebug(s"Put fiber into removal list. Fiber: ${notification.getKey}")
      cacheGuardian.addRemovalFiber(notification.getKey, notification.getValue)
      _cacheSize.addAndGet(-notification.getValue.size())
      decFiberCountAndSize(notification.getKey, 1, notification.getValue.size())
    }
  }

  private val weigher = new Weigher[FiberId, FiberCache] {
    override def weigh(key: FiberId, value: FiberCache): Int = {
      // We should calculate the weigh with the occupied size of the block.
      math.ceil(value.getOccupiedSize() / KB).toInt
    }
  }

  // this is only used when enable index and data cache separation
  private var dataCacheInstance = if (indexDataSeparationEnable) {
    initLoadingCache(DATA_MAX_WEIGHT)
  } else {
    null
  }

  // this is only used when enable index and data cache separation
  private var indexCacheInstance = if (indexDataSeparationEnable) {
    initLoadingCache(INDEX_MAX_WEIGHT)
  } else {
    null
  }

  // this is only used when disable index and data cache separation
  private var generalCacheInstance = if (!indexDataSeparationEnable) {
    initLoadingCache(TOTAL_MAX_WEIGHT)
  } else {
    null
  }

  private def initLoadingCache(weight: Int) = {
    CacheBuilder.newBuilder()
      .recordStats()
      .removalListener(removalListener)
      .maximumWeight(weight)
      .weigher(weigher)
      .concurrencyLevel(CONCURRENCY_LEVEL)
      .build[FiberId, FiberCache](new CacheLoader[FiberId, FiberCache] {
      override def load(key: FiberId): FiberCache = {
        val startLoadingTime = System.currentTimeMillis()
        val fiberCache = cache(key)
        incFiberCountAndSize(key, 1, fiberCache.size())
        logDebug(
          "Load missed index fiber took %s. Fiber: %s. length: %s".format(
            Utils.getUsedTimeMs(startLoadingTime), key, fiberCache.size()))
        _cacheSize.addAndGet(fiberCache.size())
        fiberCache
      }
    })
  }

  override def get(fiber: FiberId): FiberCache = {
    val readLock = OapRuntime.getOrCreate.fiberLockManager.getFiberLock(fiber).readLock()
    readLock.lock()
    try {
        if (indexDataSeparationEnable) {
          if (fiber.isInstanceOf[DataFiberId] || fiber.isInstanceOf[TestDataFiberId]) {
            val fiberCache = dataCacheInstance.get(fiber)
            // Avoid loading a fiber larger than DATA_MAX_WEIGHT / CONCURRENCY_LEVEL
            assert(fiberCache.size() <= DATA_MAX_WEIGHT * KB / CONCURRENCY_LEVEL,
              s"Failed to cache fiber(${Utils.bytesToString(fiberCache.size())}) " +
                s"with cache's MAX_WEIGHT" +
                s"(${Utils.bytesToString(DATA_MAX_WEIGHT.toLong * KB.toLong)}) " +
                s"/ $CONCURRENCY_LEVEL")
            fiberCache.occupy()
            fiberCache
          } else if (
            fiber.isInstanceOf[BTreeFiberId] ||
              fiber.isInstanceOf[BitmapFiberId] ||
              fiber.isInstanceOf[TestIndexFiberId]) {
            val fiberCache = indexCacheInstance.get(fiber)
            // Avoid loading a fiber larger than INDEX_MAX_WEIGHT / CONCURRENCY_LEVEL
            assert(fiberCache.size() <= INDEX_MAX_WEIGHT * KB / CONCURRENCY_LEVEL,
              s"Failed to cache fiber(${Utils.bytesToString(fiberCache.size())}) " +
                s"with cache's MAX_WEIGHT" +
                s"(${Utils.bytesToString(INDEX_MAX_WEIGHT.toLong * KB.toLong)}) " +
                s"/ $CONCURRENCY_LEVEL")
            fiberCache.occupy()
            fiberCache
          } else throw new OapException(s"not support fiber type $fiber")
        } else {
          val fiberCache = generalCacheInstance.get(fiber)
          // Avoid loading a fiber larger than MAX_WEIGHT / CONCURRENCY_LEVEL
          assert(fiberCache.size() <= TOTAL_MAX_WEIGHT * KB / CONCURRENCY_LEVEL,
            s"Failed to cache fiber(${Utils.bytesToString(fiberCache.size())}) " +
              s"with cache's MAX_WEIGHT" +
              s"(${Utils.bytesToString(TOTAL_MAX_WEIGHT.toLong * KB.toLong)}) / $CONCURRENCY_LEVEL")
          fiberCache.occupy()
          fiberCache
        }
    } finally {
      readLock.unlock()
    }
  }

  override def put(fiber: FiberCache): Unit = {
    throw new OapException("Unsupported in SimpleCache")
  }

  override def getIfPresent(fiber: FiberId): FiberCache =
    if (indexDataSeparationEnable) {
      if (fiber.isInstanceOf[DataFiberId] || fiber.isInstanceOf[TestDataFiberId]) {
        dataCacheInstance.getIfPresent(fiber)
      } else if (
        fiber.isInstanceOf[BTreeFiberId] ||
          fiber.isInstanceOf[BitmapFiberId] ||
          fiber.isInstanceOf[TestIndexFiberId]) {
        indexCacheInstance.getIfPresent(fiber)
      } else null
    } else {
      generalCacheInstance.getIfPresent(fiber)
    }

  override def getFibers: Set[FiberId] =
    if (indexDataSeparationEnable) {
      dataCacheInstance.asMap().keySet().asScala.toSet ++
        indexCacheInstance.asMap().keySet().asScala.toSet
    } else {
      generalCacheInstance.asMap().keySet().asScala.toSet
    }

  override def invalidate(fiber: FiberId): Unit =
    if (indexDataSeparationEnable) {
      if (fiber.isInstanceOf[DataFiberId] || fiber.isInstanceOf[TestDataFiberId]) {
        dataCacheInstance.invalidate(fiber)
      } else if (
        fiber.isInstanceOf[BTreeFiberId] ||
          fiber.isInstanceOf[BitmapFiberId] ||
          fiber.isInstanceOf[TestIndexFiberId]) {
        indexCacheInstance.invalidate(fiber)
      }
    } else {
      generalCacheInstance.invalidate(fiber)
    }

  override def invalidateAll(fibers: Iterable[FiberId]): Unit = {
    fibers.foreach(invalidate)
  }

  override def cacheSize: Long = _cacheSize.get()

  override def cacheStats: CacheStats = {
    if (indexDataSeparationEnable) {
      val dataStats = dataCacheInstance.stats()
      val indexStats = indexCacheInstance.stats()
      CacheStats(
        dataFiberCount.get(), dataFiberSize.get(),
        indexFiberCount.get(), indexFiberSize.get(),
        pendingFiberCount, cacheGuardian.pendingFiberSize,
        dataStats.hitCount(),
        dataStats.missCount(),
        dataStats.loadCount(),
        dataStats.totalLoadTime(),
        dataStats.evictionCount(),
        indexStats.hitCount(),
        indexStats.missCount(),
        indexStats.loadCount(),
        indexStats.totalLoadTime(),
        indexStats.evictionCount()
      )
    } else {
      // when disable index and data cache separation
      // we can't independently retrieve index and data cache
      val cacheStats = generalCacheInstance.stats()
      CacheStats(
        dataFiberCount.get(), dataFiberSize.get(),
        indexFiberCount.get(), indexFiberSize.get(),
        pendingFiberCount, cacheGuardian.pendingFiberSize,
        cacheStats.hitCount(),
        cacheStats.missCount(),
        cacheStats.loadCount(),
        cacheStats.totalLoadTime(),
        cacheStats.evictionCount(),
        0L,
        0L,
        0L,
        0L,
        0L
      )
    }
  }

  override def cacheCount: Long =
    if (indexDataSeparationEnable) {
      dataCacheInstance.size() + indexCacheInstance.size()
    } else {
      generalCacheInstance.size()
    }

  override def pendingFiberCount: Int = cacheGuardian.pendingFiberCount

  override def cleanUp(): Unit = {
    super.cleanUp()
    if (indexDataSeparationEnable) {
      dataCacheInstance.cleanUp()
      indexCacheInstance.cleanUp()
    } else {
      generalCacheInstance.cleanUp()
    }
  }

  // This is only for test purpose
  private[filecache] def enableCacheSeparation(): Unit = {
    this.indexDataSeparationEnable = true
    if (dataCacheInstance == null) {
      dataCacheInstance = initLoadingCache(DATA_MAX_WEIGHT)
    }
    if (indexCacheInstance == null) {
      indexCacheInstance = initLoadingCache(INDEX_MAX_WEIGHT)
    }
    generalCacheInstance = null
  }
}
