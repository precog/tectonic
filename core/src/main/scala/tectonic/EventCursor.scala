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

import scala.{sys, Array, Boolean, Int, Long, StringContext, Unit}
import scala.annotation.switch

import java.lang.{CharSequence, SuppressWarnings}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.FinalVal",
    "org.wartremover.warts.PublicInference",
    "org.wartremover.warts.Var"))
final class EventCursor private (
    tagBuffer: Array[Long],
    tagLimit: Int,
    tagSubshiftLimit: Int,
    strsBuffer: Array[CharSequence],
    strsLimit: Int,
    intsBuffer: Array[Int],
    intsLimit: Int) {

  private[this] final var tagCursor: Int = 0
  private[this] final var tagSubshiftCursor: Int = 0
  private[this] final var strsCursor: Int = 0
  private[this] final var intsCursor: Int = 0

  // TODO skips
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  def next(plate: Plate[_]): Boolean = {
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
      case 0xB => plate.finishRow()
      case 0xC => plate.skipped(nextInt())
      case tag => sys.error(s"assertion failed: unrecognized tag = $tag")
    }

    !(tagCursor == tagLimit && tagSubshiftCursor == tagSubshiftLimit)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private[this] final def nextTag(): Int = {
    val back = ((tagBuffer(tagCursor) >>> tagSubshiftCursor) & 0xF).toInt

    if (tagSubshiftCursor == 60) {
      tagCursor += 1
      tagSubshiftCursor = 0
    } else {
      tagSubshiftCursor += 4
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
    tagSubshiftCursor = 0
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
      tagSubshiftLimit: Int,
      strsBuffer: Array[CharSequence],
      strsLimit: Int,
      intsBuffer: Array[Int],
      intsLimit: Int)
      : EventCursor =
    new EventCursor(
      tagBuffer,
      tagLimit,
      tagSubshiftLimit,
      strsBuffer,
      strsLimit,
      intsBuffer,
      intsLimit)
}
