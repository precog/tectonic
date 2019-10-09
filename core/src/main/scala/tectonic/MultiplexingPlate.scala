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

import scala._

import java.lang.CharSequence

class MultiplexingPlate[A] private (main: Plate[A], side: Plate[Unit])
    extends Plate[A] {

  private[this] val sig = Signal

  def arr(): Signal =
    sig.and(side.arr(), main.arr())

  def map(): Signal =
    sig.and(side.map(), main.map())

  def fls(): Signal =
    sig.and(side.fls(), main.fls())

  def tru(): Signal =
    sig.and(side.tru(), main.tru())

  def nul(): Signal =
    sig.and(side.nul(), main.nul())

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal =
    sig.and(
      side.num(s, decIdx, expIdx),
      main.num(s, decIdx, expIdx))

  def str(s: CharSequence): Signal =
    sig.and(side.str(s), main.str(s))

  def nestArr(): Signal =
    sig.and(side.nestArr(), main.nestArr())

  def nestMap(pathComponent: CharSequence): Signal =
    sig.and(
      side.nestMap(pathComponent),
      main.nestMap(pathComponent))

  def nestMeta(pathComponent: CharSequence): Signal =
    sig.and(
      side.nestMeta(pathComponent),
      main.nestMeta(pathComponent))

  def unnest(): Signal =
    sig.and(side.unnest(), main.unnest())

  def skipped(bytes: Int): Unit = {
    side.skipped(bytes)
    main.skipped(bytes)
  }

  // only the main channel can produce results
  // but the side channel might need to side effect here
  def finishBatch(terminal: Boolean): A = {
    side.finishBatch(terminal)
    main.finishBatch(terminal)
  }

  def finishRow(): Unit = {
    side.finishRow()
    main.finishRow()
  }
}

object MultiplexingPlate {
  def apply[A](main: Plate[A], side: Plate[Unit]): Plate[A] =
    new MultiplexingPlate(main, side)
}
