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
package test

import cats.effect.IO

import org.specs2.ScalaCheck
import org.specs2.mutable._

import scala.{Boolean, Int, List, Option, Predef, Unit}, Predef._

import java.lang.{CharSequence, Runtime}

object ReplayPlateSpecs extends Specification with ScalaCheck {
  import Generators._

  "ReplayPlate" should {
    "round-trip events" in prop { (driver: ∀[λ[α => Plate[α] => Unit]]) =>
      val plate = ReplayPlate[IO](-1).unsafeRunSync()
      driver[Option[EventCursor]](plate)

      val streamOpt = plate.finishBatch(true)
      streamOpt must beSome
      val stream = streamOpt.get

      val eff = for {
        resultP <- ReifiedTerminalPlate[IO](false)
        expectedP <- ReifiedTerminalPlate[IO](false)

        _ <- IO(stream.drive(resultP))
        result <- IO(resultP.finishBatch(true))
        _ <- IO(driver[List[Event]](expectedP))
        expected <- IO(expectedP.finishBatch(true))
      } yield (result, expected)

      val (result, expected) = eff.unsafeRunSync()
      result mustEqual expected
    }.set(minTestsOk = 10000, workers = Runtime.getRuntime.availableProcessors())

    "correctly the buffers" in {
      val plate = ReplayPlate[IO](-1).unsafeRunSync()

      (0 until 131072 + 1) foreach { _ =>
        plate.nul()
      }

      val cursor = plate.finishBatch(true).get
      var counter = 0

      val sink = new Plate[Unit] {

        def nul() = {
          counter += 1
          Signal.Continue
        }

        def fls(): Signal = Signal.Continue
        def tru(): Signal = Signal.Continue
        def map(): Signal = Signal.Continue
        def arr(): Signal = Signal.Continue
        def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = Signal.Continue
        def str(s: CharSequence): Signal = Signal.Continue

        def nestMap(pathComponent: CharSequence): Signal = Signal.Continue
        def nestArr(): Signal = Signal.Continue
        def nestMeta(pathComponent: CharSequence): Signal = Signal.Continue

        def unnest(): Signal = Signal.Continue

        def finishRow(): Unit = ()
        def finishBatch(terminal: Boolean) = ()
      }

      cursor.drive(sink)

      counter mustEqual (131072 + 1)
    }
  }
}
