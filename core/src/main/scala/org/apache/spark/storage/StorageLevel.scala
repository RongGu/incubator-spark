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

package org.apache.spark.storage

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

/**
 * Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
 * or Tachyon, whether to drop the RDD to disk if it falls out of memory or Tachyon , whether to 
 * keep the data in memory in a serialized format, and whether to replicate the RDD partitions on 
 * multiple nodes.
 * The [[org.apache.spark.storage.StorageLevel$]] singleton object contains some static constants
 * for commonly useful storage levels. To create your own storage level object, use the
 * factory method of the singleton object (`StorageLevel(...)`).
 */
class StorageLevel private(
    private var useTachyon_ : Boolean,
    private var useDisk_ : Boolean,
    private var useMemory_ : Boolean,
    private var deserialized_ : Boolean,
    private var replication_ : Int = 1)
  extends Externalizable {

  // TODO: Also add fields for caching priority, dataset ID, and flushing.
  private def this(flags: Int, replication: Int) {
    this((flags & 8) != 0, (flags & 4) != 0, (flags & 2) != 0, (flags & 1) != 0, replication)
  }

  def this() = this(false, false, true, false)  // For deserialization

  def useTachyon = useTachyon_
  def useDisk = useDisk_
  def useMemory = useMemory_

  def deserialized = deserialized_
  def replication = replication_

  assert(replication < 40, "Replication restricted to be less than 40 for calculating hashcodes")

  override def clone(): StorageLevel = new StorageLevel(
    this.useTachyon, this.useDisk, this.useMemory, this.deserialized, this.replication)

  override def equals(other: Any): Boolean = other match {
    case s: StorageLevel =>
      s.useTachyon == useTachyon &&
      s.useDisk == useDisk &&
      s.useMemory == useMemory &&
      s.deserialized == deserialized &&
      s.replication == replication
    case _ =>
      false
  }

  def isValid = ((useTachyon || useMemory || useDisk) && (replication > 0))

  def toInt: Int = {
    var ret = 0
    if (useTachyon_) {
      ret |= 8
    }
    if (useDisk_) {
      ret |= 4
    }
    if (useMemory_) {
      ret |= 2
    }
    if (deserialized_) {
      ret |= 1
    }
    ret
  }

  override def writeExternal(out: ObjectOutput) {
    out.writeByte(toInt)
    out.writeByte(replication_)
  }

  override def readExternal(in: ObjectInput) {
    val flags = in.readByte()
    useTachyon_ = (flags & 8) != 0
    useDisk_ = (flags & 4) != 0
    useMemory_ = (flags & 2) != 0
    deserialized_ = (flags & 1) != 0
    replication_ = in.readByte()
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = StorageLevel.getCachedStorageLevel(this)

  override def toString: String = "StorageLevel(%b, %b, %b, %b, %d)".format(
    useTachyon, useDisk, useMemory, deserialized, replication)

  override def hashCode(): Int = toInt * 41 + replication
  def description : String = {
    var result = ""
    result += (if (useTachyon) "Tachyon " else "")
    result += (if (useDisk) "Disk " else "")
    result += (if (useMemory) "Memory " else "")
    result += (if (deserialized) "Deserialized " else "Serialized ")
    result += "%sx Replicated".format(replication)
    result
  }
}


/**
 * Various [[org.apache.spark.storage.StorageLevel]] defined and utility functions for creating
 * new storage levels.
 */
object StorageLevel {
  
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(false, true, false, false)
  val DISK_ONLY_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_ONLY = new StorageLevel(false, false, true, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, false, true, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, false, true, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, false, true, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(false, true, true, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(false, true, true, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(false, true, true, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(false, true, true, false, 2)
  
  val TACHYON = new StorageLevel(true, false, false, false)
  
  /** Create a new StorageLevel object */
  def apply(useTachyon: Boolean, useDisk: Boolean, useMemory: Boolean, 
    deserialized: Boolean, replication: Int = 1) = getCachedStorageLevel(
          new StorageLevel(useDisk, useMemory, useTachyon, deserialized, replication))

  /** Create a new StorageLevel object from its integer representation */
  def apply(flags: Int, replication: Int) =
    getCachedStorageLevel(new StorageLevel(flags, replication))

  /** Read StorageLevel object from ObjectInput stream */
  def apply(in: ObjectInput) = {
    val obj = new StorageLevel()
    obj.readExternal(in)
    getCachedStorageLevel(obj)
  }

  private[spark]
  val storageLevelCache = new java.util.concurrent.ConcurrentHashMap[StorageLevel, StorageLevel]()

  private[spark] def getCachedStorageLevel(level: StorageLevel): StorageLevel = {
    storageLevelCache.putIfAbsent(level, level)
    storageLevelCache.get(level)
  }
}
