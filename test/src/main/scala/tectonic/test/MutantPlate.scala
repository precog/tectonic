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
package test

import scala._
import scala.Predef._

import java.lang.CharSequence

import cats.effect.Sync

final class MutantPlate private () extends Plate[Unit] {

  var mutant: String = ""

  def nul(): Signal = {
    mutant += "nul"
    Signal.Continue
  }

  def fls(): Signal = {
    mutant += "fls"
    Signal.Continue
  }

  def tru(): Signal = {
    mutant += "tru"
    Signal.Continue
  }

  def map(): Signal = {
    mutant += "map"
    Signal.Continue
  }

  def arr(): Signal = {
    mutant += "arr"
    Signal.Continue
  }

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    mutant += "num" + s"${s.toString}"
    Signal.Continue
  }

  def str(s: CharSequence): Signal = {
    mutant += "str" + s"${s.toString}"
    Signal.Continue
  }

  def nestMap(pathComponent: CharSequence): Signal = {
    mutant += "nestMap" + s"${pathComponent.toString}"
    Signal.Continue
  }

  def nestArr(): Signal = {
    mutant += "nestArr"
    Signal.Continue
  }

  def nestMeta(pathComponent: CharSequence): Signal = {
    mutant += "nestMeta" + s"${pathComponent.toString}"
    Signal.Continue
  }

  def unnest(): Signal = {
    mutant += "unnest"
    Signal.Continue
  }

  def finishRow(): Unit = {
    mutant += "finishRow"
  }

  def finishBatch(terminal: Boolean): Unit = {
    mutant += "finishBatch" + s"$terminal"
  }

  override def skipped(bytes: Int): Unit = {
    mutant += "skipped" + s"$bytes"
  }
}

object MutantPlate {

  def apply[F[_]: Sync](): F[MutantPlate] =
    Sync[F].delay(new MutantPlate())
}
