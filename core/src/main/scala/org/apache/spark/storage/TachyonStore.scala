package org.apache.spark.storage

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.LinkedHashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Logging
import org.apache.spark.util.Utils
import java.io.FileOutputStream
import java.nio.channels.FileChannel.MapMode
import tachyon.client.OutStream
import tachyon.client.WriteType
import tachyon.client.ReadType
import tachyon.client.InStream

private class Entry(val size: Long)
/**
 * Stores BlockManager blocks on Tachyon.
 */
private class TachyonStore(blockManager: BlockManager,
  tachyonManager: TachyonBlockManager, maxMemory: Long)
  extends BlockStore(blockManager: BlockManager) with Logging {

  private val entries = new LinkedHashMap[BlockId, Entry](32, 0.75f, true)
  @volatile private var currentMemory = 0L
  logInfo("TachyonStore started with capacity %s.".format(Utils.bytesToString(maxMemory)))
  private val putLock = new Object()

  def freeMemory: Long = maxMemory - currentMemory

  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel) {
    putToTachyonStore(blockId, _bytes)
  }

  override def putValues(
    blockId: BlockId,
    values: ArrayBuffer[Any],
    level: StorageLevel,
    returnValues: Boolean): PutResult = {
    logDebug("Attempting to write values for block " + blockId)
    val _bytes = blockManager.dataSerialize(blockId, values.iterator)

    putLock.synchronized {
      if (ensureFreeSpace(blockId, _bytes.limit())) {
        putToTachyonStore(blockId, _bytes)
        if (returnValues) {
          PutResult(_bytes.limit(), Right(_bytes.duplicate()))
        } else {
          PutResult(_bytes.limit(), null)
        }
      } else {
        // Tell the block manager that we couldn't put it in memory so that it can drop it to disk
        //       blockManager.dropFromTachyonStore(blockId, _bytes)
        PutResult(_bytes.limit(), Right(_bytes.duplicate()))
      }
    }
  }

  private def putToTachyonStore(blockId: BlockId, _bytes: ByteBuffer) {
    // So that we do not modify the input offsets !
    //duplicate does not copy buffer, so inexpensive
    val bytes = _bytes.duplicate()
    bytes.rewind()
    logDebug("Attempting to put block " + blockId)
    val startTime = System.currentTimeMillis
    val file = tachyonManager.getFile(blockId)
    val os = file.getOutStream(WriteType.MUST_CACHE);
    os.write(bytes.array())
    os.close()
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file in Tachyon in %d ms".format(
      blockId, Utils.bytesToString(bytes.limit), (finishTime - startTime)))
    currentMemory += bytes.limit()
    entries.put(blockId, new Entry(bytes.limit()))
  }

  /**
   * Tries to free up a given amount of space to store a particular block, but can fail and return
   * false if either the block is bigger than our TachyonStore's size or it would require replacing another
   * block from the same RDD (which leads to a wasteful cyclic replacement pattern for RDDs that
   * don't fit into Tachyon that we want to avoid).
   *
   * Assumes that a lock is held by the caller to ensure only one thread is dropping blocks.
   * Otherwise, the freed space may fill up before the caller puts in their new value.
   */
  private def ensureFreeSpace(blockIdToAdd: BlockId, space: Long): Boolean = {
    logInfo(blockIdToAdd + " ensureFreeSpace(%d) called with curMem=%d, maxMem=%d in TachyonStore".format(
      space, currentMemory, maxMemory))

    if (space > maxMemory) {
      logInfo("Will not store " + blockIdToAdd + " as it is larger than our tachyon store limit")
      return false
    }

    if (maxMemory - currentMemory < space) {
      val rddToAdd = getRddId(blockIdToAdd)
      val selectedBlocks = new ArrayBuffer[BlockId]()
      var selectedMemory = 0L

      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        while (maxMemory - (currentMemory - selectedMemory) < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey
          if (rddToAdd != null && rddToAdd == getRddId(blockId)) {
            logInfo("Will not store " + blockIdToAdd + " as it would require dropping another " +
              "block from the same RDD")
            return false
          }
          selectedBlocks += blockId
          selectedMemory += pair.getValue.size
        }
      }

      if (maxMemory - (currentMemory - selectedMemory) >= space) {
        logInfo(selectedBlocks.size + " blocks selected for dropping")
        for (blockId <- selectedBlocks) {
          getBytes(blockId) match {
            case Some(bytes) =>
              blockManager.dropFromTachyonStore(blockId, bytes)
            case _ =>
              logDebug("Block " + blockId + " not found in tachyon store")
          }
          entries.remove(blockId)
        }
        return true
      } else {
        return false
      }
    }
    return true
  }

  /**
   * Return the RDD ID that a given block ID is from, or null if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): String = {
    if (blockId.isRDD) {
      blockId.asRDDId.get.rddId.toString
    } else {
      null
    }
  }

  override def remove(blockId: BlockId): Boolean = {
    val fileSegment = tachyonManager.getBlockLocation(blockId)
    val file = fileSegment.file
    if (tachyonManager.existFile(file) && file.length() == fileSegment.length) {
      tachyonManager.removeFile(file)
    } else {
      if (fileSegment.length < file.length()) {
        logWarning("Could not delete block associated with only a part of a file: " + blockId)
      }
      false
    }
    entries.synchronized {
      val entry = entries.remove(blockId)
      if (entry != null) {
        currentMemory -= entry.size
        logInfo("Block %s of size %d dropped from TachyonStroe (free %d)".format(
          blockId, entry.size, freeMemory))
        true
      } else {
        false
      }
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val segment = tachyonManager.getBlockLocation(blockId)
    val file = tachyonManager.getFile(blockId)
    val is = file.getInStream(ReadType.CACHE)
    var buffer : ByteBuffer = null
    if(is != null){
    	val size = segment.length - segment.offset
    	val bs = new Array[Byte](size.asInstanceOf[Int])
    	is.read(bs, segment.offset.asInstanceOf[Int] , size.asInstanceOf[Int])
    	buffer = ByteBuffer.wrap(bs) 
    } 
    Some(buffer)
  }

  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }
}
