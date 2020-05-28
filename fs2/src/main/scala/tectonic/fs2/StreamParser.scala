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
package fs2

import cats.Foldable
import cats.effect.Sync
import cats.implicits._

import _root_.fs2.{Chunk, Pipe, Pull, Stream}

import scala.{Byte, Int, None, Option, Some, Unit}
import scala.collection.mutable

object StreamParser {

  /**
   * Returns a transducer which parses a byte stream according to the specified
   * parser, which may be constructed effectfully. Any parse errors will be sequenced
   * into the stream as a `tectonic.ParseException`, halting consumption.
   */
  def apply[F[_]: Sync, A, B](
      parserF: F[BaseParser[F, A]])(
      chunk: A => Chunk[B])
      : Pipe[F, Byte, B] = { in =>

    def handleResult[R](pr: ParseResult[A])(next: Int => Pull[F, B, R]): Pull[F, B, R] = pr match {
      case ParseResult.Failure(e) =>
        Pull.raiseError[F](e)

      case ParseResult.Partial(a, remaining2) =>
        Pull.output(chunk(a)) >> next(remaining2)

      case ParseResult.Complete(a) =>
        Pull.output(chunk(a)) >> next(0)
    }

    def loop(in: Stream[F, Byte], parser: BaseParser[F, A], remaining: Int, last: Option[Int]): Pull[F, B, Unit] = {
      // if we successfully consumed less than half of our previous chunk, don't pull the next one: just re-churn
      if (last.map(_ / 2 < remaining).getOrElse(false)) {
        Pull.eval(parser.continue).flatMap(handleResult(_)(loop(in, parser, _, last)))
      } else {
        in.pull.uncons flatMap {
          case Some((chunk: Chunk.ByteBuffer, tail)) =>
            Pull.eval(parser.absorb(chunk.buf)).flatMap(handleResult(_)(loop(tail, parser, _, Some(chunk.size))))

          case Some((chunk, tail)) =>
            Pull.eval(parser.absorb(chunk.toByteBuffer)).flatMap(handleResult(_)(loop(tail, parser, _, Some(chunk.size))))

          case None =>
            Pull.done
        }
      }
    }

    val pull = Pull.eval(parserF) flatMap { parser =>
      lazy val finishF: Pull[F, B, Unit] =
        Pull.eval(parser.finish).flatMap(handleResult(_)(rem => if (rem <= 0) Pull.done else finishF))

      loop(in, parser, 0, None) >> finishF
    }

    pull.stream
  }

  def foldable[F[_]: Sync, G[_]: Foldable, A](parserF: F[BaseParser[F, G[A]]]): Pipe[F, Byte, A] =
    apply(parserF)(ga => Chunk.buffer(ga.foldLeft(new mutable.ArrayBuffer[A])(_ += _)))
}
