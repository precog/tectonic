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

import org.specs2.execute.Result
import org.specs2.matcher.{Matcher, MatchersImplicits}

import tectonic.csv.Parser

import scala.{Array, PartialFunction, StringContext}
import scala.util.{Left, Right}

import java.lang.{String, SuppressWarnings}

// TODO collapse with json package object
package object csv {
  private object MatchersImplicits extends MatchersImplicits

  import MatchersImplicits._

  @SuppressWarnings(Array("org.wartremover.warts.ImplicitParameter"))
  def parseAs(expected: Event*)(implicit config: Parser.Config): Matcher[String] = { input: String =>
    val resultsF = for {
      parser <- Parser(ReifiedTerminalPlate[IO], config)
      left <- parser.absorb(input)
      right <- parser.finish
    } yield (left, right)

    resultsF.unsafeRunSync() match {
      case (Right(init), Right(tail)) =>
        val results = init ++ tail
        (results == expected.toList, s"$results != ${expected.toList}")

      case (Left(err), _) =>
        (false, s"failed to parse with error '${err.getMessage}' at ${err.line}:${err.col} (i=${err.index})")

      case (_, Left(err)) =>
        (false, s"failed to parse with error '${err.getMessage}' at ${err.line}:${err.col} (i=${err.index})")
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.ImplicitParameter"))
  def failParseWithError(errorPF: PartialFunction[ParseException, Result])(implicit config: Parser.Config): Matcher[String] = { input: String =>
    val resultsF = for {
      parser <- Parser(ReifiedTerminalPlate[IO], config)
      left <- parser.absorb(input)
      right <- parser.finish
    } yield left.flatMap(xs => right.map(xs ::: _))

    resultsF.unsafeRunSync() match {
      case Left(e) if errorPF.isDefinedAt(e) =>
        val r = errorPF(e)
        (r.isSuccess, r.message)

      case Left(e) =>
        (false, s"parsed to an error, but $e was unmatched by the specified pattern")

      case Right(results) =>
        (false, s"expected error on input '$input', but successfully parsed as $results")
    }
  }

}
