/*
 * Copyright 2014–2018 SlamData Inc.
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

import scala.{math, sys, Array, Byte, Int, List, Long, StringContext, Predef, Unit}, Predef._
import scala.annotation.switch
import scala.collection.mutable

import java.lang.{AssertionError, CharSequence, IllegalArgumentException, SuppressWarnings, System}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.FinalVal",
    "org.wartremover.warts.PublicInference",
    "org.wartremover.warts.Var"))
final class EventCursor private (
    tagBuffer: Array[Long],
    tagLimit: Int,
    tagSubShiftLimit: Int,
    strsBuffer: Array[CharSequence],
    strsLimit: Int,
    intsBuffer: Array[Int],
    intsLimit: Int) {

  private[this] final var tagCursor: Int = 0
  private[this] final var tagSubShiftCursor: Int = 0
  private[this] final var strsCursor: Int = 0
  private[this] final var intsCursor: Int = 0

  private[this] final var tagCursorMark: Int = 0
  private[this] final var tagSubShiftCursorMark: Int = 0
  private[this] final var strsCursorMark: Int = 0
  private[this] final var intsCursorMark: Int = 0

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.While"))
  def drive(plate: Plate[_]): Unit = {
    if (tagLimit > 0 || tagSubShiftLimit > 0) {
      var b: Byte = 0
      while (b == 0) {
        b = nextRow(plate)

        if (b != 1) {
          plate.finishRow()
        }
      }
    }
  }

  /**
   * Returns:
   *
   * - `0` if a row has been completed but there is still more data
   * - `1` if the data stream has terminated without ending the row
   * - `2` if the data stream has terminated *and* there is no more data
   */
  // TODO skips
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Throw"))
  def nextRow(plate: Plate[_]): Byte = {
    var continue = true
    var hasNext = !(tagCursor == tagLimit && tagSubShiftCursor == tagSubShiftLimit)
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
        case tag => sys.error(s"assertion failed: unrecognized tag = $tag")
      }

      hasNext = !(tagCursor == tagLimit && tagSubShiftCursor == tagSubShiftLimit)
    }

    if (!continue && hasNext)
      0
    else if (continue && !hasNext)
      1
    else if (!continue && !hasNext)
      2
    else
      throw new AssertionError
  }

  def length: Int = tagLimit * (64 / 4) + (tagSubShiftLimit / 4)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.MutableDataStructures",
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Throw"))
  def subdivide(bound: Int): List[EventCursor] = {
    if (bound <= 0) {
      throw new IllegalArgumentException(bound.toString)
    }

    val back = mutable.ListBuffer[EventCursor]()
    val increment = math.ceil(bound.toFloat / (64 / 4)).toInt

    (0 until (tagLimit + 1) by increment).foldLeft((0, 0)) {
      case ((so, io), b) =>
        val last = !(b < tagLimit + 1 - increment)

        val length = if (last)
          tagLimit + 1 - b
        else
          increment

        val tagBuffer2 = new Array[Long](length)
        System.arraycopy(tagBuffer, b, tagBuffer2, 0, length)

        var strCount = 0
        var intCount = 0

        if (last) {
          strCount = strsLimit - so
          intCount = intsLimit - io
        } else {
          var i = 0
          while (i < length) {
            val tag = tagBuffer2(i)

            var offset = 0
            while (offset < 64) {   // we don't have to worry about tagSubshiftLimit, since we're not here if (last)
              (((tag >>> offset) & 0xF).toInt: @switch) match {
                case 0x5 =>
                  strCount += 1
                  intCount += 2

                case 0x6 | 0x7 | 0x9 =>
                  strCount += 1

                case 0xC =>
                  intCount += 1

                case _ =>
              }

              offset += 4
            }

            i += 1
          }
        }

        val strsBuffer2 = new Array[CharSequence](strCount)
        System.arraycopy(strsBuffer, so, strsBuffer2, 0, strCount)

        val intsBuffer2 = new Array[Int](intCount)
        System.arraycopy(intsBuffer, io, intsBuffer2, 0, intCount)

        back += new EventCursor(
          tagBuffer2,
          if (last && tagSubShiftLimit < 64) length - 1 else length,
          if (last) tagSubShiftLimit else 0,
          strsBuffer2,
          strCount,
          intsBuffer2,
          intCount)

        (so + strCount, io + intCount)
    }

    back.toList
  }

  /**
   * Marks the current location for subsequent rewinding. Overwrites any previous
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
   * it resets to the beginning of the stream.
   */
  def rewind(): Unit = {
    tagCursor = tagCursorMark
    tagSubShiftCursor = tagSubShiftCursorMark
    strsCursor = strsCursorMark
    intsCursor = intsCursorMark
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private[this] final def nextTag(): Int = {
    val back = ((tagBuffer(tagCursor) >>> tagSubShiftCursor) & 0xF).toInt

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
    tagCursor = 0
    tagSubShiftCursor = 0
    strsCursor = 0
    intsCursor = 0
  }

  // this will handle disk cleanup
  def finish(): Unit = ()
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
      tagBuffer,
      tagLimit,
      tagSubShiftLimit,
      strsBuffer,
      strsLimit,
      intsBuffer,
      intsLimit)
}
