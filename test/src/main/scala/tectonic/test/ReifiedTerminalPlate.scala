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
package test

import cats.effect.Sync

import scala.{Array, Boolean, Int, List, Nil, Unit}
import scala.collection.mutable

import java.lang.{CharSequence, SuppressWarnings}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.NonUnitStatements",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.TraversableOps"))
final class ReifiedTerminalPlate private (accumToTerminal: Boolean) extends Plate[List[Event]] {
  import Event._

  private val events = new mutable.ListBuffer[Event]

  def nul(): Signal = {
    events += Nul
    Signal.Continue
  }

  def fls(): Signal = {
    events += Fls
    Signal.Continue
  }

  def tru(): Signal = {
    events += Tru
    Signal.Continue
  }

  def map(): Signal = {
    events += Map
    Signal.Continue
  }

  def arr(): Signal = {
    events += Arr
    Signal.Continue
  }

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    events += Num(s, decIdx, expIdx)
    Signal.Continue
  }

  def str(s: CharSequence): Signal = {
    events += Str(s)
    Signal.Continue
  }

  def nestMap(pathComponent: CharSequence): Signal = {
    events += NestMap(pathComponent)
    Signal.Continue
  }

  def nestArr(): Signal = {
    events += NestArr
    Signal.Continue
  }

  def nestMeta(pathComponent: CharSequence): Signal = {
    events += NestMeta(pathComponent)
    Signal.Continue
  }

  def unnest(): Signal = {
    events += Unnest
    Signal.Continue
  }

  def finishRow(): Unit = events += FinishRow

  def finishBatch(terminal: Boolean): List[Event] = {
    if (accumToTerminal || terminal) {
      val back = events.toList
      events.clear()
      back
    } else {
      Nil
    }
  }

  override def skipped(bytes: Int): Unit = {
    if (bytes > 0) {
      events += Skipped(bytes)
    }
  }
}

object ReifiedTerminalPlate {

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def apply[F[_]: Sync](accumToTerminal: Boolean = true): F[Plate[List[Event]]] =
    Sync[F].delay(new ReifiedTerminalPlate(accumToTerminal))

  def visit[F[_]: Sync, A](events: List[Event], plate: Plate[A], terminus: Boolean = true): F[A] = Sync[F] delay {
    events foreach {
      case Event.Nul => plate.nul()
      case Event.Fls => plate.fls()
      case Event.Tru => plate.tru()
      case Event.Map => plate.map()
      case Event.Arr => plate.arr()
      case Event.Num(s, decIdx, expIdx) => plate.num(s, decIdx, expIdx)
      case Event.Str(s) => plate.str(s)
      case Event.NestMap(path) => plate.nestMap(path)
      case Event.NestArr => plate.nestArr()
      case Event.NestMeta(path) => plate.nestMeta(path)
      case Event.Unnest => plate.unnest()
      case Event.FinishRow => plate.finishRow()
      case Event.Skipped(bytes) => plate.skipped(bytes)
    }

    plate.finishBatch(terminus)
  }
}
