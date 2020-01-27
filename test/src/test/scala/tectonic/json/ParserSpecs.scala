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
package json

import cats.effect.IO
import cats.implicits._

import org.specs2.mutable.Specification

import tectonic.test.{Event, ReifiedTerminalPlate}
import tectonic.test.json._

import scala.{Array, Boolean, Byte, Int, List, Nil, Unit, Predef}, Predef._
import scala.collection.mutable
import scala.util.{Either, Left, Right}

import java.lang.CharSequence

class ParserSpecs extends Specification {
  import Event._

  "utf-8 byte handling" should {
    "ignore a leading byte-order mark" in {
      val bytes = Array[Byte](0xef.toByte, 0xbb.toByte, 0xbf.toByte, '{'.toByte, '}'.toByte)
      bytes must parseRowAs(Map)
    }

    "fail if a second byte-order mark appears" in {
      val bytes = Array[Byte](0xef.toByte, 0xbb.toByte, 0xbf.toByte, '{'.toByte, 0xef.toByte, 0xbb.toByte, 0xbf.toByte, '}'.toByte)
      bytes must failToParseWith(
        ParseException("expected \" got ￯ (line 1, column 5)", 4, 1, 5))
    }
  }

  "async line-delimited parsing" should {
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

      val parser = Parser(IO(new Plate[Unit] {
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

        def skipped(bytes: Int) = ()
      }), Parser.ValueStream).unsafeRunSync()

      parser.absorb("42").unsafeRunSync() must beRight(())
      calls.toList mustEqual List(false)

      parser.finish.unsafeRunSync() must beRight(())
      calls.toList mustEqual List(false, true)
    }

    "call finishBatch with false, and then true on incomplete value" in {
      val calls = new mutable.ListBuffer[Boolean]

      val parser = Parser(IO(new Plate[Unit] {
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

        def skipped(bytes: Int) = ()
      }), Parser.ValueStream).unsafeRunSync()

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

  "column skips on nest" should {
    def targetMask[A](target: Either[Int, String])(delegate: Plate[A]): Plate[A] = new DelegatingPlate[A](delegate) {
      private[this] var depth = 0
      private[this] var index = 0

      override def nestMap(pathComponent: CharSequence): Signal = {
        if (Right(pathComponent.toString) == target && depth == 0) {
          super.nestMap(pathComponent)
        } else {
          depth += 1
          Signal.SkipColumn
        }
      }

      override def nestArr(): Signal = {
        if (depth == 0) {
          index += 1
          if (Left(index - 1) == target) {
            super.nestArr()
          } else {
            depth += 1
            Signal.SkipColumn
          }
        } else {
          depth += 1
          Signal.SkipColumn
        }
      }

      override def unnest(): Signal = {
        if (depth == 0) {
          super.unnest()
        } else {
          depth -= 1
          Signal.Continue
        }
      }
    }

    "skip .a and .c in { a: ..., b: ..., c: ... }" in {
      val input = """{ "a": 42, "b": "hi", "c": true }"""
      val expected = List(Skipped(4), NestMap("b"), Str("hi"), Unnest, Skipped(7), FinishRow)
      input must parseAsWithPlate(expected: _*)(targetMask[List[Event]](Right("b")))
    }

    "skip .a and .b in { a: { no: ..., thanks: ... }, b: ..., c: ... }" in {
      val input = """{ "a": { "no": 42, "thanks": null }, "b": "hi", "c": true }"""
      val expected = List(Skipped(30), Skipped(6), NestMap("c"), Tru, Unnest, FinishRow)
      input must parseAsWithPlate(expected: _*)(targetMask[List[Event]](Right("c")))
    }

    "retain only [1] in [..., ..., ..., ...]" in {
      val input = """[42, "hi", true, null]"""
      val expected = List(Skipped(2), NestArr, Str("hi"), Unnest, Skipped(5), Skipped(5), FinishRow)
      input must parseAsWithPlate(expected: _*)(targetMask[List[Event]](Left(1)))
    }

    "handle nested structure in skips" in {
      val input = """{ "a": { "c": [1, 2, 3], "d": { "e": null } }, "b": "hi" }"""
      val expected = List(Skipped(40), NestMap("b"), Str("hi"), Unnest, FinishRow)
      input must parseAsWithPlate(expected: _*)(targetMask[List[Event]](Right("b")))
    }

    "correctly ignore structure in skipped strings" in {
      val input = """{ "a": "foo}", "b": "hi" }"""
      val expected = List(Skipped(8), NestMap("b"), Str("hi"), Unnest, FinishRow)
      input must parseAsWithPlate(expected: _*)(targetMask[List[Event]](Right("b")))
    }

    "suspend appropriately within skips" in {
      val input1 = """{ "a": 4"""
      val input2 = """2, "b": "hi" }"""

      val expected = List(
        Skipped(1),
        NestMap("b"),
        Str("hi"),
        Unnest,
        FinishRow)

      val eff = for {
        parser <- Parser(
          ReifiedTerminalPlate[IO]().map(targetMask[List[Event]](Right("b"))),
          Parser.ValueStream)

        first <- parser.absorb(input1)
        second <- parser.absorb(input2)
        third <- parser.finish
      } yield (first, second, third)

      val (first, second, third) = eff.unsafeRunSync()

      first must beRight(List(Skipped(3)))
      second must beRight(expected)
      third must beRight(Nil: List[Event])
    }
  }

  "asynchronous state management" should {
    "ensure buffer is appropriately paged within the row" in {
      // parse ≥ 2 MB out of a single row and ensure the buffer never exceeds 1 MB + ε

      val front = "[{\"foo\":42"
      val middle = ",\"bar\":{\"baz\":[1, 2, 3, 4], \"bin\":false},\"quix\":\"hi\"},{\"foo\":42"
      val end = "\"}]"

      val middleCount = ((2 * 1024 * 1024) - front.length - end.length) / middle.length + 1
      val epsilon = front.length + middle.length + end.length

      def replicate[A](fa: IO[A], n: Int): IO[Unit] =
        if (n > 0) fa.void >> replicate(fa, n - 1) else fa.void

      val ioa = for {
        parser <- Parser[IO, Unit](IO.pure(NullPlate), Parser.ValueStream)
        _ <- parser.absorb(front)
        _ <- replicate(parser.absorb(middle).rethrow, middleCount)
        _ <- parser.absorb(end)

        len <- IO(parser.unsafeLen())

        _ <- parser.finish

        len2 <- IO(parser.unsafeLen())
      } yield (len, len2)

      val (len, len2) = ioa.unsafeRunSync()

      len must beLessThan(1024 * 1024 + epsilon)
      len2 must beLessThan(1024 * 1024 + epsilon)
    }
  }
}
