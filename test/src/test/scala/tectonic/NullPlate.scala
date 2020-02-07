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

import scala.{Boolean, Int, Unit}

import java.lang.CharSequence

object NullPlate extends Plate[Unit] {
  def arr() = Signal.Continue
  def finishBatch(terminal: Boolean) = ()
  def finishRow() = ()
  def fls() = Signal.Continue
  def map() = Signal.Continue
  def nestArr() = Signal.Continue
  def nestMap(pathComponent: CharSequence) = Signal.Continue
  def nestMeta(pathComponent: CharSequence) = Signal.Continue
  def nul() = Signal.Continue
  def num(s: CharSequence, decIdx: Int, expIdx: Int) = Signal.Continue
  def skipped(bytes: Int) = ()
  def str(s: CharSequence) = Signal.Continue
  def tru() = Signal.Continue
  def unnest() = Signal.Continue
}
