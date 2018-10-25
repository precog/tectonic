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

import org.specs2.matcher.{Matcher, MatchersImplicits}

import tectonic.csv.Parser

import scala.{Array, StringContext}
import scala.util.{Left, Right}

import java.lang.{String, SuppressWarnings}

// TODO collapse with json package object
package object csv {
  private object MatchersImplicits extends MatchersImplicits

  import MatchersImplicits._

  @SuppressWarnings(Array("org.wartremover.warts.ImplicitParameter"))
  def parseAs(expected: Event*)(implicit config: Parser.Config): Matcher[String] = { input: String =>
    val parser = Parser(new ReifiedTerminalPlate, config)

    (parser.absorb(input), parser.finish()) match {
      case (Right(init), Right(tail)) =>
        val results = init ++ tail
        (results == expected.toList, s"expected ${expected.toList} and got $results")

      case (Left(err), _) =>
        (false, s"failed to parse with error '${err.getMessage}' at ${err.line}:${err.col} (i=${err.index})")

      case (_, Left(err)) =>
        (false, s"failed to parse with error '${err.getMessage}' at ${err.line}:${err.col} (i=${err.index})")
    }
  }
}
