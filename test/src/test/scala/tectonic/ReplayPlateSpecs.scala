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
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.traverse._

import org.specs2.ScalaCheck
import org.specs2.mutable._

import scala.{math, Boolean, Int, List, Option, Predef, Unit}, Predef._

import java.lang.{CharSequence, IllegalArgumentException, IllegalStateException, Runtime}

object ReplayPlateSpecs extends Specification with ScalaCheck {
  import Generators._

  "ReplayPlate" should {
    "round-trip events" in prop { (driver: ∀[λ[α => Plate[α] => Unit]]) =>
      val plate = ReplayPlate[IO](52428800, true).unsafeRunSync()
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
      stream.length mustEqual expected.length
      result mustEqual expected
    }.set(minTestsOk = 10000, workers = Runtime.getRuntime.availableProcessors())

    "only produce one row at a time" in {
      val plate = ReplayPlate[IO](52428800, true).unsafeRunSync()
      plate.str("first")
      plate.finishRow()
      plate.str("second")
      plate.finishRow()

      val stream = plate.finishBatch(true).get

      val eff = for {
        firstP <- ReifiedTerminalPlate[IO](false)
        secondP <- ReifiedTerminalPlate[IO](false)

        row1 <- IO(stream.nextRow(firstP))
        row2 <- IO(stream.nextRow(secondP))

        firstResults <- IO(firstP.finishBatch(true))
        secondResults <- IO(secondP.finishBatch(true))
      } yield (firstResults, row1, secondResults, row2)

      val (firstResults, row1, secondResults, row2) = eff.unsafeRunSync()

      firstResults mustEqual List(Event.Str("first"))
      row1 mustEqual 0
      secondResults mustEqual List(Event.Str("second"))
      row2 mustEqual 2
    }

    "retain events when subdividing into multiple buffers" in prop { (driver: ∀[λ[α => Plate[α] => Unit]], size0: Int) =>
      (size0 > Int.MinValue) ==> {
        val plate = ReplayPlate[IO](52428800, true).unsafeRunSync()
        driver[Option[EventCursor]](plate)

        val stream = plate.finishBatch(true).get

        (stream.length > 0) ==> {
          val size = math.abs(size0) % stream.length

          if (size == 0) {
            stream.subdivide(size) must throwAn[IllegalArgumentException]
          } else {
            val partitions = stream.subdivide(size)

            partitions must haveSize(be_>=(1))
            partitions.map(countRows).sum mustEqual countRows(stream)

            val lengths = partitions.map(_.length)
            lengths.sum mustEqual stream.length
            lengths must contain(be_>=((size / 16) * 16))

            val origEff =
              ReifiedTerminalPlate[IO](false) flatMap { plate =>
                IO(stream.drive(plate)) >> IO(plate.finishBatch(true))
              }

            val partEff = partitions traverse { substream =>
              ReifiedTerminalPlate[IO](false) flatMap { plate =>
                IO(substream.drive(plate)) >> IO(plate.finishBatch(true))
              }
            }

            val originalResults = origEff.unsafeRunSync()
            val partitionResults = partEff.unsafeRunSync().flatten

            originalResults mustEqual partitionResults
          }
        }
      }
    }.set(minTestsOk = 10000, workers = Runtime.getRuntime.availableProcessors())

    "mark and rewind at arbitrary points" in {
      val plate = ReplayPlate[IO](52428800, true).unsafeRunSync()
      plate.str("first")
      plate.finishRow()
      plate.str("second")
      plate.finishRow()

      val stream = plate.finishBatch(true).get

      val eff = for {
        firstP <- ReifiedTerminalPlate[IO](false)

        _ <- IO {
          stream.nextRow(firstP)
        }

        firstResults <- IO(firstP.finishBatch(true))

        secondP <- ReifiedTerminalPlate[IO](false)

        _ <- IO {
          stream.rewind()
          stream.nextRow(secondP)
        }

        secondResults <- IO(secondP.finishBatch(true))

        thirdP <- ReifiedTerminalPlate[IO](false)

        _ <- IO {
          stream.mark()
          stream.nextRow(thirdP)
        }

        thirdResults <- IO(thirdP.finishBatch(true))

        fourthP <- ReifiedTerminalPlate[IO](false)

        _ <- IO {
          stream.rewind()
          stream.nextRow(fourthP)
        }

        fourthResults <- IO(fourthP.finishBatch(true))
      } yield (firstResults, secondResults, thirdResults, fourthResults)

      val (firstResults, secondResults, thirdResults, fourthResults) =
        eff.unsafeRunSync()

      firstResults mustEqual List(Event.Str("first"))
      secondResults mustEqual List(Event.Str("first"))
      thirdResults mustEqual List(Event.Str("second"))
      fourthResults mustEqual List(Event.Str("second"))
    }


    "measure distance during rewind" in {
      val plate = ReplayPlate[IO](52428800, true).unsafeRunSync()

      plate.str("first")
      plate.finishRow()
      plate.str("second")
      plate.finishRow()
      plate.num("42", -1, -1)
      plate.finishRow()
      plate.nestMap("key")
      plate.str("third")
      plate.unnest()
      plate.finishRow()

      val stream = plate.finishBatch(true).get

      // we're going to ignore this anyway
      val sink = ReifiedTerminalPlate[IO](false).unsafeRunSync()

      stream.nextRow(sink)   // "first"
      stream.rewind() mustEqual 2

      stream.nextRow(sink)   // "first"
      stream.mark()
      stream.nextRow(sink)   // "second"
      stream.rewind() mustEqual 2

      stream.nextRow(sink)    // "second"
      stream.nextRow(sink)    // 42
      stream.mark()
      stream.nextRow(sink)    // { "key": "third" }
      stream.rewind() mustEqual 4

      stream.nextRow(sink)    // { "key": "third" }
      stream.rewind() mustEqual 4
    }

    "correctly grow the buffers" in {
      val plate = ReplayPlate[IO](52428800, true).unsafeRunSync()

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

        def skipped(bytes: Int) = ()
      }

      cursor.drive(sink)

      counter mustEqual (131072 + 1)
    }

    "produce an error when attempting to grow beyond bounds" in {
      val plate = ReplayPlate[IO](8200, true).unsafeRunSync()

      {
        (0 until 131072 + 1) foreach { _ =>
          plate.nul()
        }
      } must throwAn[IllegalStateException]
    }
  }

  def countRows(ec: EventCursor): Int = {
    var count = if (ec.length > 0) 1 else 0

    while (ec.nextRow(NullPlate) == 0) {
      count += 1
    }

    ec.reset()

    count
  }

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
    def num(s: CharSequence,decIdx: Int,expIdx: Int) = Signal.Continue
    def skipped(bytes: Int) = ()
    def str(s: CharSequence) = Signal.Continue
    def tru() = Signal.Continue
    def unnest() = Signal.Continue
  }
}
