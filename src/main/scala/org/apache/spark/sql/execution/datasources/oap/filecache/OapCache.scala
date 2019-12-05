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

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import com.google.common.cache._

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.utils.PersistentMemoryConfigUtils
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.unsafe.VMEMCacheJNI
import org.apache.spark.util.Utils

trait OapCache {
  val dataFiberSize: AtomicLong = new AtomicLong(0)
  val indexFiberSize: AtomicLong = new AtomicLong(0)
  val dataFiberCount: AtomicLong = new AtomicLong(0)
  val indexFiberCount: AtomicLong = new AtomicLong(0)

  def get(fiber: FiberId): FiberCache
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
  private val cacheHitCount: AtomicLong = new AtomicLong(0)
  private val cacheMissCount: AtomicLong = new AtomicLong(0)

  val cacheMap : ConcurrentHashMap[FiberId, FiberCache] = new ConcurrentHashMap[FiberId, FiberCache]
  cacheGuardian.start()

  override def get(fiber: FiberId): FiberCache = {
    if (cacheMap.containsKey(fiber)) {
      cacheHitCount.getAndAdd(1)
      val fiberCache = cacheMap.get(fiber)
      fiberCache.occupy()
      fiberCache
    } else {
      cacheMissCount.getAndAdd(1)
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

  override def cacheStats: CacheStats = {
    CacheStats(_cacheCount.get(), _cacheSize.get(), 0, 0, cacheGuardian.pendingFiberCount,
      cacheGuardian.pendingFiberSize, cacheHitCount.get(), cacheMissCount.get(),
      0, 0, 0,
      0, 0, 0, 0, 0)
  }

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
    FiberCache.getEmptyDataFiberCache(fiberLength)

  private val cacheHitCount: AtomicLong = new AtomicLong(0)
  private val cacheMissCount: AtomicLong = new AtomicLong(0)
  private val cacheTotalGetTime: AtomicLong = new AtomicLong(0)
  private var cacheTotalCount: Long = 0
  private var cacheEvictCount: Long = 0
  private var cacheTotalSize: Long = 0
  // We don't bother the memory use of Simple Cache
  private val cacheGuardian = new CacheGuardian(Int.MaxValue)
  cacheGuardian.start()

  @volatile private var initialized = false
  private val lock = new Object
  private val conf = SparkEnv.get.conf
  private val initialSizeStr = conf.get(OapConf.OAP_FIBERCACHE_PERSISTENT_MEMORY_INITIAL_SIZE).trim
  private val vmInitialSize = Utils.byteStringAsBytes(initialSizeStr)
  require(vmInitialSize > 0, "AEP initial size must be greater than zero")
  def initializeVMEMCache(): Unit = {
    if (!initialized) {
      lock.synchronized {
        if (!initialized) {
          val sparkEnv = SparkEnv.get
          val conf = sparkEnv.conf
          // The NUMA id should be set when the executor process start up. However, Spark don't
          // support NUMA binding currently.
          var numaId = conf.getInt("spark.executor.numa.id", -1)
          val executorId = sparkEnv.executorId.toInt
          val map = PersistentMemoryConfigUtils.parseConfig(conf)
          if (numaId == -1) {
            logWarning(s"Executor ${executorId} is not bind with NUMA. It would be better" +
              s" to bind executor with NUMA when cache data to Intel Optane DC persistent memory.")
            // Just round the executorId to the total NUMA number.
            // TODO: improve here
            numaId = executorId % PersistentMemoryConfigUtils.totalNumaNode(conf)
          }
          val initialPath = map.get(numaId).get
          val fullPath = Utils.createTempDir(initialPath + File.separator + executorId)

          require(fullPath.isDirectory(), "VMEMCache initialize path must be a directory")
          val success = VMEMCacheJNI.initialize(fullPath.getCanonicalPath, vmInitialSize);
          if (success != 0) {
            throw new SparkException("Failed to call VMEMCacheJNI.initialize")
          }
          logInfo(s"Executors ${executorId}: VMEMCache initialize path:" +
            s" ${fullPath.getCanonicalPath}, size: ${1.0 * vmInitialSize / 1024 / 1024 / 1024}GB")
          initialized = true
        }
      }
    }
  }

  initializeVMEMCache()

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
      incFiberCountAndSize(fiber, 1, fiberCache.size())
      fiberCache.occupy()
      decFiberCountAndSize(fiber, 1, fiberCache.size())
      cacheGuardian.addRemovalFiber(fiber, fiberCache)
      fiberCache
    } else { // cache hit
      cacheHitCount.addAndGet(1)
      val length = res
      val fiberCache = emptyDataFiber(length)
      val startTime = System.currentTimeMillis()
      val get = VMEMCacheJNI.getNative(fiberKey.getBytes(), null,
        0, fiberKey.length, fiberCache.getBaseOffset, 0, fiberCache.size().toInt)
      logDebug(s"second getNative require ${length} bytes. " +
        s"returns $get bytes, takes ${System.currentTimeMillis() - startTime} ms")
      fiberCache.fiberId = fiber
      fiberCache.occupy()
      cacheGuardian.addRemovalFiber(fiber, fiberCache)
      fiberCache
    }
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

  override def cache(fiberId: FiberId): FiberCache = {
    val fiber = super.cache(fiberId)
    VMEMCacheJNI.putNative(fiberId.toFiberKey().getBytes(), null, 0,
      fiberId.toFiberKey().length, fiber.getBaseOffset,
      0, fiber.getOccupiedSize().toInt)
    fiber
  }

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
