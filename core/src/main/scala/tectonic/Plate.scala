/*
 * Copyright 2020 Precog Data
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

abstract class Plate[A] { self =>
  def nul(): Signal
  def fls(): Signal
  def tru(): Signal
  def map(): Signal
  def arr(): Signal
  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal
  def str(s: CharSequence): Signal

  def nestMap(pathComponent: CharSequence): Signal
  def nestArr(): Signal
  def nestMeta(pathComponent: CharSequence): Signal

  def unnest(): Signal

  def finishRow(): Unit
  def finishBatch(terminal: Boolean): A

  def skipped(bytes: Int): Unit

  final def mapDelegate[B](f: A => B): Plate[B] = new Plate[B] {
    def nul(): Signal = self.nul()
    def fls(): Signal = self.fls()
    def tru(): Signal = self.tru()
    def map(): Signal = self.map()
    def arr(): Signal = self.arr()
    def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = self.num(s, decIdx, expIdx)
    def str(s: CharSequence): Signal = self.str(s)

    def nestMap(pathComponent: CharSequence): Signal = self.nestMap(pathComponent)
    def nestArr(): Signal = self.nestArr()
    def nestMeta(pathComponent: CharSequence): Signal = self.nestMeta(pathComponent)

    def unnest(): Signal = self.unnest()

    def finishRow(): Unit = self.finishRow()

    override def finishBatch(terminal: Boolean): B =
      f(self.finishBatch(terminal))

    override def skipped(bytes: Int): Unit = self.skipped(bytes)
  }
}
