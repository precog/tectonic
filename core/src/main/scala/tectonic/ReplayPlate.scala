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

import cats.effect.Sync

import scala.{Array, Boolean, Int, Long, None, Option, Some, StringContext, Unit}
// import scala.{Predef}, Predef._

import java.lang.{CharSequence, IllegalStateException, SuppressWarnings, System}

/**
 * Produces None until finishBatch(true) is called.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.PublicInference",
    "org.wartremover.warts.FinalVal"))
final class ReplayPlate private (limit: Int, retainSkips: Boolean) extends Plate[Option[EventCursor]] {

  // everything fits into a word
  private[this] final val Nul = EventCursor.Nul
  private[this] final val Fls = EventCursor.Fls
  private[this] final val Tru = EventCursor.Tru
  private[this] final val Map = EventCursor.Map
  private[this] final val Arr = EventCursor.Arr
  private[this] final val Num = EventCursor.Num
  private[this] final val Str = EventCursor.Str
  private[this] final val NestMap = EventCursor.NestMap
  private[this] final val NestArr = EventCursor.NestArr
  private[this] final val NestMeta = EventCursor.NestMeta
  private[this] final val Unnest = EventCursor.Unnest
  private[this] final val FinishRow = EventCursor.FinishRow
  private[this] final val Skipped = EventCursor.Skipped

  private[this] final val Continue = Signal.Continue

  private[this] var tagBuffer = new Array[Long](ReplayPlate.DefaultBufferSize)
  private[this] var tagPointer = 0
  private[this] var tagSubShift = 0

  private[this] var strsBuffer = new Array[CharSequence](ReplayPlate.DefaultBufferSize / 2)
  private[this] var strsPointer = 0

  private[this] var intsBuffer = new Array[Int](ReplayPlate.DefaultBufferSize / 16)
  private[this] var intsPointer = 0

  final def nul(): Signal = {
    appendTag(Nul)
    Continue
  }

  final def fls(): Signal = {
    appendTag(Fls)
    Continue
  }

  final def tru(): Signal = {
    appendTag(Tru)
    Continue
  }

  final def map(): Signal = {
    appendTag(Map)
    Continue
  }

  final def arr(): Signal = {
    appendTag(Arr)
    Continue
  }

  final def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    appendTag(Num)
    appendStr(s)
    appendInt(decIdx)
    appendInt(expIdx)
    Continue
  }

  final def str(s: CharSequence): Signal = {
    appendTag(Str)
    appendStr(s)
    Continue
  }

  final def nestMap(pathComponent: CharSequence): Signal = {
    appendTag(NestMap)
    appendStr(pathComponent)
    Continue
  }

  final def nestArr(): Signal = {
    appendTag(NestArr)
    Continue
  }

  final def nestMeta(pathComponent: CharSequence): Signal = {
    appendTag(NestMeta)
    appendStr(pathComponent)
    Continue
  }

  final def unnest(): Signal = {
    appendTag(Unnest)
    Continue
  }

  final def finishRow(): Unit = appendTag(FinishRow)

  final def finishBatch(terminal: Boolean): Option[EventCursor] = {
    if (terminal)
      Some(
        EventCursor(
          tagBuffer,
          tagPointer,
          tagSubShift,
          strsBuffer,
          strsPointer,
          intsBuffer,
          intsPointer))
    else
      None
  }

  override final def skipped(bytes: Int): Unit =
    if (retainSkips) {
      appendTag(Skipped)
      appendInt(bytes)
    } else {
      ()
    }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private[this] final def appendTag(tag: Int): Unit = {
    checkTags()
    tagBuffer(tagPointer) |= tag.toLong << tagSubShift

    if (tagSubShift == 60) {
      tagPointer += 1
      tagSubShift = 0
    } else {
      tagSubShift += 4
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Throw"))
  private[this] final def checkTags(): Unit = {
    if (tagSubShift == 0 && tagPointer >= tagBuffer.length) {
      if (tagBuffer.length * 2 > limit) {
        throw new IllegalStateException(s"unable to grow EventCursor beyond $limit")
      }

      val tagBuffer2 = new Array[Long](tagBuffer.length * 2)
      System.arraycopy(tagBuffer, 0, tagBuffer2, 0, tagBuffer.length)
      tagBuffer = tagBuffer2
    }
  }

  private[this] final def appendStr(cs: CharSequence): Unit = {
    checkStrs()
    strsBuffer(strsPointer) = cs
    strsPointer += 1
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Throw"))
  private[this] final def checkStrs(): Unit = {
    if (strsPointer >= strsBuffer.length) {
      if (strsBuffer.length * 2 > limit) {
        throw new IllegalStateException(s"unable to grow EventCursor beyond $limit")
      }

      val strsBuffer2 = new Array[CharSequence](strsBuffer.length * 2)
      System.arraycopy(strsBuffer, 0, strsBuffer2, 0, strsBuffer.length)
      strsBuffer = strsBuffer2
    }
  }

  private[this] final def appendInt(i: Int): Unit = {
    checkInts()
    intsBuffer(intsPointer) = i
    intsPointer += 1
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Throw"))
  private[this] final def checkInts(): Unit = {
    if (intsPointer >= intsBuffer.length) {
      if (intsBuffer.length * 2 > limit) {
        throw new IllegalStateException(s"unable to grow EventCursor beyond $limit")
      }

      val intsBuffer2 = new Array[Int](intsBuffer.length * 2)
      System.arraycopy(intsBuffer, 0, intsBuffer2, 0, intsBuffer.length)
      intsBuffer = intsBuffer2
    }
  }
}

object ReplayPlate {
  // this gives us 512 events, which is ample to start, and we'll grow to 8 kb in 7 doubles
  // generally it just makes us much more efficient in the singleton cartesian case, and not much less efficient in the massive case
  val DefaultBufferSize: Int = 32

  def apply[F[_]: Sync](limit: Int, retainSkips: Boolean): F[Plate[Option[EventCursor]]] =
    Sync[F].delay(new ReplayPlate(limit, retainSkips))
}
