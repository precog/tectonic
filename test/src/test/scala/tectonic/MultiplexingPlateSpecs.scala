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

import scala._

import cats.effect.IO
import org.specs2.mutable.Specification

object MultiplexingPlateSpecs extends Specification {

  "MultiplexingPlate" should {
    "retain state" >> {
      "arr" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.arr()
        mutant.mutant mustEqual "arr"
        plate.finishBatch(true) mustEqual List(Event.Arr)
      }

      "map" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.map()
        mutant.mutant mustEqual "map"
        plate.finishBatch(true) mustEqual List(Event.Map)
      }

      "fls" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.fls()
        mutant.mutant mustEqual "fls"
        plate.finishBatch(true) mustEqual List(Event.Fls)
      }

      "tru" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.tru()
        mutant.mutant mustEqual "tru"
        plate.finishBatch(true) mustEqual List(Event.Tru)
      }

      "nul" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.nul()
        mutant.mutant mustEqual "nul"
        plate.finishBatch(true) mustEqual List(Event.Nul)
      }

      "num" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.num("42", 0, 0)
        mutant.mutant mustEqual "num42"
        plate.finishBatch(true) mustEqual List(Event.Num("42", 0, 0))
      }

      "str" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.str("foo")
        mutant.mutant mustEqual "strfoo"
        plate.finishBatch(true) mustEqual List(Event.Str("foo"))
      }

      "nestArr" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.nestArr()
        mutant.mutant mustEqual "nestArr"
        plate.finishBatch(true) mustEqual List(Event.NestArr)
      }

      "nestMap" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.nestMap("foo")
        mutant.mutant mustEqual "nestMapfoo"
        plate.finishBatch(true) mustEqual List(Event.NestMap("foo"))
      }

      "nestMeta" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.nestMeta("foo")
        mutant.mutant mustEqual "nestMetafoo"
        plate.finishBatch(true) mustEqual List(Event.NestMeta("foo"))
      }

      "unnest" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.unnest
        mutant.mutant mustEqual "unnest"
        plate.finishBatch(true) mustEqual List(Event.Unnest)
      }

      "skipped" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.skipped(12)
        mutant.mutant mustEqual "skipped12"
        plate.finishBatch(true) mustEqual List(Event.Skipped(12))
      }

      "finishBatch" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.finishBatch(true) mustEqual List()
        mutant.mutant mustEqual "finishBatchtrue"
      }

      "finishRow" in {
        val mutant = MutantPlate[IO]().unsafeRunSync()
        val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()

        val plate = MultiplexingPlate[List[Event]](reified, mutant)

        plate.finishRow()
        mutant.mutant mustEqual "finishRow"
        plate.finishBatch(true) mustEqual List(Event.FinishRow)
      }
    }
  }
}
