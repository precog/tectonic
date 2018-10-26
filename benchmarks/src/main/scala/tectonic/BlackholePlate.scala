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

import scala.collection.immutable.List

import org.openjdk.jmh.infra.Blackhole

final class BlackholePlate(
    vectorCost: Long,
    scalarCost: Long,
    tinyScalarCost: Long,
    numericCost: Long,
    rowCost: Long,
    batchCost: Long) extends Plate[List[Nothing]] {

  import Blackhole.consumeCPU

  def nul(): Signal = {
    consumeCPU(tinyScalarCost)
    Signal.Continue
  }

  def fls(): Signal = {
    consumeCPU(tinyScalarCost)
    Signal.Continue
  }

  def tru(): Signal = {
    consumeCPU(tinyScalarCost)
    Signal.Continue
  }

  def map(): Signal = {
    consumeCPU(tinyScalarCost)
    Signal.Continue
  }

  def arr(): Signal = {
    consumeCPU(tinyScalarCost)
    Signal.Continue
  }

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    if (decIdx < 0 && expIdx < 0)
      consumeCPU(scalarCost)
    else
      consumeCPU(numericCost)

    Signal.Continue
  }

  def str(s: CharSequence): Signal = {
    consumeCPU(scalarCost)
    Signal.Continue
  }

  def nestMap(pathComponent: CharSequence): Signal = {
    consumeCPU(vectorCost)
    Signal.Continue
  }

  def nestArr(): Signal = {
    consumeCPU(vectorCost)
    Signal.Continue
  }

  def nestMeta(pathComponent: CharSequence): Signal = {
    consumeCPU(vectorCost)
    Signal.Continue
  }

  def unnest(): Signal = {
    consumeCPU(vectorCost)
    Signal.Continue
  }

  def finishRow(): Unit =
    consumeCPU(rowCost)

  def finishBatch(terminal: Boolean): List[Nothing] = {
    consumeCPU(batchCost)
    List.empty[Nothing]
  }
}
