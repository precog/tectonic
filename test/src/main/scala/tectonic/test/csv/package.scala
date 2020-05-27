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
import cats.implicits._

import org.specs2.execute.Result
import org.specs2.matcher.{Matcher, MatchersImplicits}

import tectonic.csv.Parser

import scala.{PartialFunction, StringContext}

import java.lang.String

// TODO collapse with json package object
package object csv {
  private object MatchersImplicits extends MatchersImplicits

  import MatchersImplicits._

  def parseAs(expected: Event*)(implicit config: Parser.Config): Matcher[String] = { input: String =>
    val resultsF = for {
      parser <- Parser(ReifiedTerminalPlate[IO](), config)
      left <- parser.absorb(input)
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

  def failParseWithError(errorPF: PartialFunction[ParseException, Result])(implicit config: Parser.Config): Matcher[String] = { input: String =>
    val resultsF = for {
      parser <- Parser(ReifiedTerminalPlate[IO](), config)
      left <- parser.absorb(input)
      right <- parser.finish
    } yield left.flatMap(xs => right.map(xs ::: _))

    resultsF.unsafeRunSync() match {
      case ParseResult.Failure(e) if errorPF.isDefinedAt(e) =>
        val r = errorPF(e)
        (r.isSuccess, r.message)

      case ParseResult.Failure(e) =>
        (false, s"parsed to an error, but $e was unmatched by the specified pattern")

      case ParseResult.Complete(results) =>
        (false, s"expected error on input '$input', but successfully parsed as $results")
    }
  }

}
