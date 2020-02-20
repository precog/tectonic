/*
 * Copyright 2014–2020 SlamData Inc.
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

import java.lang.CharSequence

import scala.{Boolean, Int, Unit}

abstract class DelegatingPlate[A](val delegate: Plate[A]) extends Plate[A] {

  def nul(): Signal =
    delegate.nul()

  def fls(): Signal =
    delegate.fls()

  def tru(): Signal =
    delegate.tru()

  def map(): Signal =
    delegate.map()

  def arr(): Signal =
    delegate.arr()

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal =
    delegate.num(s, decIdx, expIdx)

  def str(s: CharSequence): Signal =
    delegate.str(s)

  def nestMap(pathComponent: CharSequence): Signal =
    delegate.nestMap(pathComponent)

  def nestArr(): Signal =
    delegate.nestArr()

  def nestMeta(pathComponent: CharSequence): Signal =
    delegate.nestMeta(pathComponent)

  def unnest(): Signal =
    delegate.unnest()

  def finishRow(): Unit =
    delegate.finishRow()

  def finishBatch(terminal: Boolean): A =
    delegate.finishBatch(terminal)

  override def skipped(bytes: Int): Unit =
    delegate.skipped(bytes)
}
