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

import cats.instances.int._
import cats.syntax.eq._

import scala._, Predef._
import scala.annotation.switch
import scala.collection.mutable

import java.lang.{AssertionError, CharSequence, IllegalArgumentException, SuppressWarnings}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.FinalVal",
    "org.wartremover.warts.PublicInference",
    "org.wartremover.warts.Var"))
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
   * - `0` if a row has ended but there is still more data
   * - `1` if the data stream has terminated without ending the row
   * - `2` if the data stream has terminated *and* the row has ended
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

  def length: Int =
    (tagLimit * (64 / 4) + (tagSubShiftLimit / 4)) - (tagOffset * (64 / 4) + (tagSubShiftOffset / 4))

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
    val semanticMax = tagLimit * (64 / 4) + (tagSubShiftLimit / 4)

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def pushNext(tagFrom: Int, tagFromSubShift: Int, strsFrom: Int, intsFrom: Int, cheatUp: Boolean): Unit = {
      println(s"tagFrom = $tagFrom")
      println(s"tagFromSubShift = $tagFromSubShift")
      println(s"cheatUp = $cheatUp")

      if (tagFrom < tagLimit || tagFromSubShift < tagSubShiftLimit) {
        val semanticTargetIndex =
          math.min((tagFrom * (64 / 4)) + (tagFromSubShift / 4) + bound, semanticMax)

        val idealTagTargetIndex = semanticTargetIndex / (64 / 4)

        println(s"idealTagTargetIndex = $idealTagTargetIndex")

        def attemptTarget(tagTargetIndex: Int, offsetBound: Int) = {
          val targetRowOffset = if (tagTargetIndex == tagLimit)
            tagSubShiftLimit
          else
            findRowBoundary(tagBuffer(tagTargetIndex), offsetBound)

          if (tagTargetIndex <= tagLimit && targetRowOffset >= 0) {
            val tagOffset2 = tagFrom
            val tagSubShiftOffset2 = tagFromSubShift

            // TODO tagLimit2 - 1?
            val (tagLimit2, tagSubShiftLimit2) = if (tagTargetIndex == tagLimit)
              (tagLimit, tagSubShiftLimit)
            else if (targetRowOffset == 60)
              (tagTargetIndex + 1, 0)
            else
              (tagTargetIndex, targetRowOffset + 4)

            val strsOffset2 = strsFrom
            val intsOffset2 = intsFrom

            val (strsCount, intsCount) =
              countStrsInts(tagOffset2, tagSubShiftOffset2, tagLimit2, tagSubShiftLimit2)

            val strsLimit2 = strsOffset2 + strsCount
            val intsLimit2 = intsOffset2 + intsCount

            back += new EventCursor(
              tagBuffer = tagBuffer,
              tagOffset = tagOffset2,
              tagSubShiftOffset = tagSubShiftOffset2,
              tagLimit = tagLimit2,
              tagSubShiftLimit = tagSubShiftLimit2,
              strsBuffer = strsBuffer,
              strsOffset = strsOffset2,
              strsLimit = strsLimit2,
              intsBuffer = intsBuffer,
              intsOffset = intsOffset2,
              intsLimit = intsLimit2)

            Some((tagLimit2, tagSubShiftLimit2, strsLimit2, intsLimit2))
          } else {
            None
          }
        }

        val idealResults = attemptTarget(
          idealTagTargetIndex,
          if (idealTagTargetIndex == tagFrom) tagFromSubShift else 0)

        def iterateFit(lowerTagTargetIndex: Int, upperTagTargetIndex: Int): Unit = {
          val results = if (upperTagTargetIndex <= tagLimit) {
            if (lowerTagTargetIndex > tagFrom) {
              if (cheatUp)
                attemptTarget(upperTagTargetIndex, 0).orElse(attemptTarget(lowerTagTargetIndex, 0))
              else
                attemptTarget(lowerTagTargetIndex, 0).orElse(attemptTarget(upperTagTargetIndex, 0))
            } else {
              attemptTarget(upperTagTargetIndex, 0)
            }
          } else if (lowerTagTargetIndex > tagFrom) {
            attemptTarget(lowerTagTargetIndex, 0)
          } else {
            sys.error("impossible; this means we walked off the end and didn't realize it")
          }

          results match {
            case Some((tagLimit2, tagSubShiftLimit2, strsLimit2, intsLimit2)) =>
              pushNext(
                tagLimit2,
                tagSubShiftLimit2,
                strsLimit2,
                intsLimit2,
                tagLimit2 == lowerTagTargetIndex)

            case None =>
              iterateFit(lowerTagTargetIndex - 1, upperTagTargetIndex + 1)
          }
        }

        // TODO deduplicate?
        idealResults match {
          case Some((tagLimit2, tagSubShiftLimit2, strsLimit2, intsLimit2)) =>
            pushNext(
              tagLimit2,
              tagSubShiftLimit2,
              strsLimit2,
              intsLimit2,
              cheatUp)

          case None =>
            iterateFit(idealTagTargetIndex - 1, idealTagTargetIndex + 1)
        }
      }
    }

    pushNext(tagOffset, tagSubShiftOffset, strsOffset, intsOffset, true)

    back.toList
  }

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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Return"))
  private[this] final def findRowBoundary(tag: Long, initOffset: Int): Int = {
    var offset = initOffset
    while (offset < 64) {
      if (((tag >>> offset) & 0xF).toInt == 0xB) {
        return offset
      }

      offset += 4
    }

    -1
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.While"))
  private[this] final def countStrsInts(
      from: Int,
      fromSubShift: Int,
      to: Int,
      toSubShift: Int): (Int, Int) = {

    var strsCount = 0
    var intsCount = 0

    val limit = if (toSubShift == 0) to else to + 1
    var i = from
    var offset = fromSubShift

    while (i < limit) {
      val tag = tagBuffer(i)

      val offsetBound = if (i == limit - 1 && toSubShift != 0)
        toSubShift
      else
        64

      while (offset < offsetBound) {
        (((tag >>> offset) & 0xF).toInt: @switch) match {
          case 0x5 =>
            strsCount += 1
            intsCount += 2

          case 0x6 | 0x7 | 0x9 =>
            strsCount += 1

          case 0xC =>
            intsCount += 1

          case _ =>
        }

        offset += 4
      }

      offset = 0
      i += 1
    }

    (strsCount, intsCount)
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
    tagCursor = tagOffset
    tagSubShiftCursor = tagSubShiftOffset
    strsCursor = strsOffset
    intsCursor = intsOffset
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
}
