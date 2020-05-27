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

import cats.effect.IO

import org.specs2.matcher.{Matcher, MatchersImplicits}

import tectonic.json.Parser

import scala.{List, StringContext}

package object json {
  private object MatchersImplicits extends MatchersImplicits

  import MatchersImplicits._

  def parseRowAs[A: Absorbable](expected: Event*): Matcher[A] =
    parseAs(expected :+ Event.FinishRow: _*)

  def parseAs[A: Absorbable](expected: Event*): Matcher[A] =
    parseAsWithPlate(expected: _*)(p => p)

  def parseAsWithPlate[A: Absorbable](expected: Event*)(f: Plate[List[Event]] => Plate[List[Event]]): Matcher[A] = { input: A =>
    val resultsF = for {
      parser <- Parser(ReifiedTerminalPlate[IO]().map(f), Parser.ValueStream)
      left <- Absorbable[A].absorb(parser, input)
      right <- parser.finish
    } yield (left, right)

    resultsF.unsafeRunSync() match {
      case (ParseResult.Complete(init), ParseResult.Complete(tail)) =>
        val results = init ++ tail
        (results == expected.toList, s"$results != ${expected.toList}")

      case (ParseResult.Partial(a, remaining), _) =>
        (false, s"left partially succeded with partial result $a and $remaining bytes remaining")

      case (_, ParseResult.Partial(a, remaining)) =>
        (false, s"right partially succeded with partial result $a and $remaining bytes remaining")

      case (ParseResult.Failure(err), _) =>
        (false, s"failed to parse with error '${err.getMessage}' at ${err.line}:${err.col} (i=${err.index})")

      case (_, ParseResult.Failure(err)) =>
        (false, s"failed to parse with error '${err.getMessage}' at ${err.line}:${err.col} (i=${err.index})")
    }
  }

  def failToParseWith[A: Absorbable](expected: ParseException): Matcher[A] = { input: A =>
    val resultsF = for {
      parser <- Parser(ReifiedTerminalPlate[IO](), Parser.ValueStream)
      left <- Absorbable[A].absorb(parser, input)
      right <- parser.finish
    } yield (left, right)

    resultsF.unsafeRunSync() match {
      case (ParseResult.Complete(_), ParseResult.Complete(_)) =>
        (false, s"blergh", s"input parsed successfully (expected failure)")

      case (ParseResult.Partial(a, remaining), _) =>
        (false, "", s"left partially succeded with partial result $a and $remaining bytes remaining")

      case (_, ParseResult.Partial(a, remaining)) =>
        (false, "", s"right partially succeded with partial result $a and $remaining bytes remaining")

      case (ParseResult.Failure(err), _) =>
        (err == expected, s"input failed to parse and $err == $expected", s"input failed to parse but $err != $expected")

      case (_, ParseResult.Failure(err)) =>
        (err == expected, s"input failed to parse and $err == $expected", s"input failed to parse but $err != $expected")

    }
  }
}
