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
package json

import cats.effect.IO

import org.specs2.mutable.Specification

import tectonic.test.Event
import tectonic.test.json._

import scala.{Array, Boolean, Int, List, Unit, Predef}, Predef._
import scala.collection.mutable

import java.lang.{CharSequence, SuppressWarnings}

@SuppressWarnings(Array("org.wartremover.warts.Equals"))
object ParserSpecs extends Specification {

  "async line-delimited parsing" should {
    import Event._

    "parse all of the scalars" >> {
      "null" >> {
        "null" must parseRowAs(Nul)
      }

      "false" >> {
        "false" must parseRowAs(Fls)
      }

      "true" >> {
        "true" must parseRowAs(Tru)
      }

      "{}" >> {
        "{}" must parseRowAs(Map)
      }

      "[]" >> {
        "[]" must parseRowAs(Arr)
      }

      "number" >> {
        "integral" >> {
          "42" must parseRowAs(Num("42", -1, -1))
        }

        "decimal" >> {
          "3.1415" must parseRowAs(Num("3.1415", 1, -1))
        }

        "exponential" >> {
          "2.99792458e8" must parseRowAs(Num("2.99792458e8", 1, 10))
        }
      }

      "string" >> {
        """"quick brown fox"""" must parseRowAs(Str("quick brown fox"))
      }
    }

    "parse a map with two keys" in {
      """{"a":123, "b": false}""" must parseRowAs(
        NestMap("a"),
        Num("123", -1, -1),
        Unnest,
        NestMap("b"),
        Fls,
        Unnest)
    }

    "parse a map within a map" in {
      """{"a": {"b": null }   }""" must parseRowAs(
        NestMap("a"),
        NestMap("b"),
        Nul,
        Unnest,
        Unnest)
    }

    "parse an array with four values" in {
      """["a", 123, "b", false]""" must parseRowAs(
        NestArr,
        Str("a"),
        Unnest,
        NestArr,
        Num("123", -1, -1),
        Unnest,
        NestArr,
        Str("b"),
        Unnest,
        NestArr,
        Fls,
        Unnest)
    }

    "parse two rows of scalars" in {
      """12 true""" must parseAs(Num("12", -1, -1), FinishRow, Tru, FinishRow)
    }

    "parse two rows of non-scalars" in {
      """{"a": 3.14} {"b": false, "c": "abc"}""" must parseAs(
        NestMap("a"),
        Num("3.14", 1, -1),
        Unnest,
        FinishRow,
        NestMap("b"),
        Fls,
        Unnest,
        NestMap("c"),
        Str("abc"),
        Unnest,
        FinishRow)
    }

    "call finishBatch with false, and then true on complete value" in {
      val calls = new mutable.ListBuffer[Boolean]

      val parser = Parser[IO](new Plate[Unit] {
        def nul(): Signal = Signal.Continue
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
        def finishBatch(terminal: Boolean): Unit = calls += terminal
      }, Parser.ValueStream).unsafeRunSync()

      parser.absorb("42").unsafeRunSync() must beRight(())
      calls.toList mustEqual List(false)

      parser.finish.unsafeRunSync() must beRight(())
      calls.toList mustEqual List(false, true)
    }

    "call finishBatch with false, and then true on incomplete value" in {
      val calls = new mutable.ListBuffer[Boolean]

      val parser = Parser[IO](new Plate[Unit] {
        def nul(): Signal = Signal.Continue
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
        def finishBatch(terminal: Boolean): Unit = calls += terminal
      }, Parser.ValueStream).unsafeRunSync()

      parser.absorb("\"h").unsafeRunSync() must beRight(())
      calls.toList mustEqual List(false)

      parser.absorb("i\"").unsafeRunSync() must beRight(())
      calls.toList mustEqual List(false, false)

      parser.finish.unsafeRunSync() must beRight(())
      calls.toList mustEqual List(false, false, true)
    }

    "handle arbitrarily nested arrays" >> {
      "1" >> {
        "[[1]]" must parseRowAs(NestArr, NestArr, Num("1", -1, -1), Unnest, Unnest)
      }

      "63" >> {
        val input =
          (0 until 63).map(_ => '[').mkString +
            "1" +
            (0 until 63).map(_ => ']').mkString

        val output =
          (0 until 63).map(_ => NestArr) ++
            List(Num("1", -1, -1)) ++
            (0 until 63).map(_ => Unnest)

        input must parseRowAs(output: _*)
      }

      "64" >> {
        val input =
          (0 until 64).map(_ => '[').mkString +
            "1" +
            (0 until 64).map(_ => ']').mkString

        val output =
          (0 until 64).map(_ => NestArr) ++
            List(Num("1", -1, -1)) ++
            (0 until 64).map(_ => Unnest)

        input must parseRowAs(output: _*)
      }

      "65" >> {
        val input =
          (0 until 65).map(_ => '[').mkString +
            "1" +
            (0 until 65).map(_ => ']').mkString

        val output =
          (0 until 65).map(_ => NestArr) ++
            List(Num("1", -1, -1)) ++
            (0 until 65).map(_ => Unnest)

        input must parseRowAs(output: _*)
      }

      "100" >> {
        val input =
          (0 until 100).map(_ => '[').mkString +
            "1" +
            (0 until 100).map(_ => ']').mkString

        val output =
          (0 until 100).map(_ => NestArr) ++
            List(Num("1", -1, -1)) ++
            (0 until 100).map(_ => Unnest)

        input must parseRowAs(output: _*)
      }
    }
  }
}
