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

import cats.effect.IO

import org.specs2.matcher.{BeEqualTo, Expectable, Matcher, MatchResult}

import scala.StringContext

package object test {

  def replayAs(events: Event*): Matcher[EventCursor] = {
    new BeEqualTo(events.toList) ^^ { ec: EventCursor =>
      val plate = ReifiedTerminalPlate[IO]().unsafeRunSync()
      ec.drive(plate)
      plate.finishBatch(true)
    }
  }

  def beComplete[A](a: A): Matcher[ParseResult[A]] =
    new Matcher[ParseResult[A]] {
      def apply[B <: ParseResult[A]](exp: Expectable[B]): MatchResult[B] = exp.value match {
        case ParseResult.Complete(`a`) =>
          Matcher.result[B](
            true,
            s"${exp.description} was Complete and result value matched",
            "",
            exp)

        case ParseResult.Complete(b) =>
          Matcher.result[B](
            false,
            "",
            s"${exp.description} was Complete but $a != $b",
            exp)

        case ParseResult.Partial(a, remaining) =>
          Matcher.result[B](
            false,
            "",
            s"${exp.description} was only partially consumed (resulting in $a with $remaining bytes remaining)",
            exp)

        case ParseResult.Failure(e) =>
          Matcher.result[B](
            false,
            "",
            s"${exp.description} resulted in a failure with error $e",
            exp)
      }
    }

  // def bePartial[A](a: A, rem: Int): Matcher[ParseResult[A]]
}
