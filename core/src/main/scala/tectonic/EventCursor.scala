/*
 * Copyright 2014–2019 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tectonic

import cats.instances.int._
import cats.syntax.eq._

import scala._, Predef._
import scala.annotation.switch

import java.lang.{AssertionError, CharSequence}

final class EventCursor private (
    tagBuffer: Array[Long],
    tagOffset: Int,
    tagSubShiftOffset: Int,
    tagLimit: Int,
    tagSubShiftLimit: Int,
    strsBuffer: Array[CharSequence],
    strsOffset: Int,
    strsLimit: Int,
    intsBuffer: Array[Int],
    intsOffset: Int,
    intsLimit: Int) {

  private[this] final var tagCursor: Int = tagOffset
  private[this] final var tagSubShiftCursor: Int = tagSubShiftOffset
  private[this] final var strsCursor: Int = strsOffset
  private[this] final var intsCursor: Int = intsOffset

  private[this] final var tagCursorMark: Int = tagOffset
  private[this] final var tagSubShiftCursorMark: Int = tagSubShiftOffset
  private[this] final var strsCursorMark: Int = strsOffset
  private[this] final var intsCursorMark: Int = intsOffset

  private[this] final var tagCursorBatchStart: Int = tagOffset
  private[this] final var tagSubShiftCursorBatchStart: Int = tagSubShiftOffset
  private[this] final var strsCursorBatchStart: Int = strsOffset
  private[this] final var intsCursorBatchStart: Int = intsOffset

  private[this] final val NextRow = EventCursor.NextRowStatus.NextRow
  private[this] final val NextBatch = EventCursor.NextRowStatus.NextBatch
  private[this] final val NextRowAndBatch = EventCursor.NextRowStatus.NextRowAndBatch

  def drive(plate: Plate[_]): Unit = {
    if (tagLimit > 0 || tagSubShiftLimit > 0) {
      var b: EventCursor.NextRowStatus = NextRow
      while (b == NextRow) {
        b = nextRow(plate)

        if (b != NextBatch) {
          plate.finishRow()
        }
      }
    }
  }

  /**
   * Originally returned: (TODO remove this documentation)
   *
   * - `0` ==> NextRow
   * - `1` ==> NextBatch
   * - `2` ==> NextRowAndBatch
   */
  // TODO skips
  def nextRow(plate: Plate[_]): EventCursor.NextRowStatus = {
    var continue = true
    var hasNext = this.hasNext()    // we use a var rather than relying on the def so that we can *set* it to be false on end of batch

    while (continue && hasNext) {
      // we can't use @switch if we use vals here :-(
      val _ = (nextTag(): @switch) match {
        case 0x0 => plate.nul()
        case 0x1 => plate.fls()
        case 0x2 => plate.tru()
        case 0x3 => plate.map()
        case 0x4 => plate.arr()
        case 0x5 => plate.num(nextStr(), nextInt(), nextInt())
        case 0x6 => plate.str(nextStr())
        case 0x7 => plate.nestMap(nextStr())
        case 0x8 => plate.nestArr()
        case 0x9 => plate.nestMeta(nextStr())
        case 0xA => plate.unnest()
        case 0xB => continue = false
        case 0xC => plate.skipped(nextInt())

        // we define end-of-batch to be analogous to end-of-stream
        case 0xD => hasNext = false

        case tag => sys.error(s"assertion failed: unrecognized tag = ${tag.toHexString}")
      }

      hasNext = hasNext && this.hasNext()
    }

    if (!continue && hasNext)
      NextRow
    else if (continue && !hasNext)
      NextBatch
    else if (!continue && !hasNext)
      NextRowAndBatch
    else
      throw new AssertionError
  }

  /**
   * Sets the current batch window to to start wherever the cursor is
   * currently pointing. Does not advance the cursor. Returns true if
   * there actually *is* a batch here (even if the batch is empty),
   * returns false if at EOF.
   */
  def establishBatch(): Boolean = {
    if (hasNext()) {
      tagCursorBatchStart = tagCursor
      tagSubShiftCursorBatchStart = tagSubShiftCursor
      strsCursorBatchStart = strsCursor
      intsCursorBatchStart = intsCursor

      tagCursorMark = tagCursorBatchStart
      tagSubShiftCursorMark = tagSubShiftCursorBatchStart
      strsCursorMark = strsCursorBatchStart
      intsCursorMark = intsCursorBatchStart

      true
    } else {
      false
    }
  }

  def length: Int =
    (tagLimit * (64 / 4) + (tagSubShiftLimit / 4)) - (tagOffset * (64 / 4) + (tagSubShiftOffset / 4))

  /**
   * Marks the cursor location for subsequent rewinding. Overwrites any previous
   * mark.
   */
  def mark(): Unit = {
    tagCursorMark = tagCursor
    tagSubShiftCursorMark = tagSubShiftCursor
    strsCursorMark = strsCursor
    intsCursorMark = intsCursor
  }

  /**
   * Rewinds the cursor location to the last mark. If no mark has been set,
   * it resets to the beginning of the stream. Returns the number of events
   * rewound.
   */
  def rewind(): Int = {
    val tagCursorDistance = tagCursor - tagCursorMark
    val tagSubShiftDistance = tagSubShiftCursor - tagSubShiftCursorMark

    tagCursor = tagCursorMark
    tagSubShiftCursor = tagSubShiftCursorMark
    strsCursor = strsCursorMark
    intsCursor = intsCursorMark

    (tagCursorDistance * (64 / 4) + (tagSubShiftDistance / 4))
  }

  private[this] final def hasNext(): Boolean =
    !(tagCursor == tagLimit && tagSubShiftCursor == tagSubShiftLimit)

  private[this] final def currentTag(): Int =
    ((tagBuffer(tagCursor) >>> tagSubShiftCursor) & 0xF).toInt

  private[this] final def nextTag(): Int = {
    val back = currentTag()

    if (tagSubShiftCursor == 60) {
      tagCursor += 1
      tagSubShiftCursor = 0
    } else {
      tagSubShiftCursor += 4
    }

    back
  }

  private[this] final def nextStr(): CharSequence = {
    val back = strsBuffer(strsCursor)
    strsCursor += 1
    back
  }

  private[this] final def nextInt(): Int = {
    val back = intsBuffer(intsCursor)
    intsCursor += 1
    back
  }

  def reset(): Unit = {
    tagCursor = tagCursorBatchStart
    tagSubShiftCursor = tagSubShiftCursorBatchStart
    strsCursor = strsCursorBatchStart
    intsCursor = intsCursorBatchStart
  }

  def copy(): EventCursor = {
    new EventCursor(
      tagBuffer = tagBuffer,
      tagOffset = tagOffset,
      tagSubShiftOffset = tagSubShiftOffset,
      tagLimit = tagLimit,
      tagSubShiftLimit = tagSubShiftLimit,
      strsBuffer = strsBuffer,
      strsOffset = strsOffset,
      strsLimit = strsLimit,
      intsBuffer = intsBuffer,
      intsOffset = intsOffset,
      intsLimit = intsLimit)
  }

  // this will handle disk cleanup
  def finish(): Unit = ()

  override def toString: String = {
    def padTo(in: String, length: Int): String = {
      if (in.length === length)
        in
      else
        (0 until (in.length - length)).map(_ => '0').mkString ++ in
    }

    val adjustment = if (tagSubShiftLimit > 0) 1 else 0
    val projectedTags = tagBuffer.take(tagLimit + adjustment).drop(tagOffset)

    if (projectedTags.isEmpty) {
      s"EventCursor(tagLimit = $tagLimit, tagSubShiftLimit = $tagSubShiftLimit, tagOffset = $tagOffset, tagSubShiftOffset = $tagSubShiftOffset)"
    } else {
      val head = projectedTags(0) // >>> tagSubShiftOffset
      val tail = projectedTags.tail

      val headStr = padTo(head.toHexString.toUpperCase, 16 /*- (tagSubShiftOffset / 4)*/).reverse
      val tailStr = tail.map(_.toHexString.toUpperCase).map(padTo(_, 16).reverse).mkString

      s"EventCursor(tags = ${headStr + tailStr}, tagLimit = $tagLimit, tagSubShiftLimit = $tagSubShiftLimit, tagOffset = $tagOffset, tagSubShiftOffset = $tagSubShiftOffset)"
    }
  }
}

object EventCursor {
  private[tectonic] val Nul = 0x0
  private[tectonic] val Fls = 0x1
  private[tectonic] val Tru = 0x2
  private[tectonic] val Map = 0x3
  private[tectonic] val Arr = 0x4
  private[tectonic] val Num = 0x5
  private[tectonic] val Str = 0x6
  private[tectonic] val NestMap = 0x7
  private[tectonic] val NestArr = 0x8
  private[tectonic] val NestMeta = 0x9
  private[tectonic] val Unnest = 0xA
  private[tectonic] val FinishRow = 0xB
  private[tectonic] val Skipped = 0xC
  private[tectonic] val EndBatch = 0xD    // special columnar boundary terminator

  private[tectonic] def apply(
      tagBuffer: Array[Long],
      tagLimit: Int,
      tagSubShiftLimit: Int,
      strsBuffer: Array[CharSequence],
      strsLimit: Int,
      intsBuffer: Array[Int],
      intsLimit: Int)
      : EventCursor =
    new EventCursor(
      tagBuffer = tagBuffer,
      tagOffset = 0,
      tagSubShiftOffset = 0,
      tagLimit = tagLimit,
      tagSubShiftLimit = tagSubShiftLimit,
      strsBuffer = strsBuffer,
      strsOffset = 0,
      strsLimit = strsLimit,
      intsBuffer = intsBuffer,
      intsOffset = 0,
      intsLimit = intsLimit)

  sealed trait NextRowStatus extends Product with Serializable

  object NextRowStatus {
    case object NextRow extends NextRowStatus
    case object NextBatch extends NextRowStatus
    case object NextRowAndBatch extends NextRowStatus
  }
}

// 🦄
// [events..., FinishRow, FinishBatch]
// [events..., FinishRow]
// [events..., FinishRow, events...]
// [events..., FinishBatch]
// [events...]

// :-(
// [events..., FinishRow]
// [events..., FinishRow, events...]
// [events...]
