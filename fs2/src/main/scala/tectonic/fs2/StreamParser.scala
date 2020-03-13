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

import cats.{Foldable, Functor}
import cats.effect.Sync
import cats.evidence.As
import cats.instances.list._
import cats.syntax.all._

import _root_.fs2.{Chunk, Pipe, Stream}

import scala.{Array, Byte, List}
import scala.collection.mutable
import scala.util.Either

import java.lang.{SuppressWarnings, Throwable}

object StreamParser {

  /**
   * Returns a transducer which parses a byte stream according to the specified
   * parser, which may be constructed effectfully. Any parse errors will be sequenced
   * into the stream as a `tectonic.ParseException`, halting consumption.
   */
  @SuppressWarnings(Array(
    "org.wartremover.warts.NonUnitStatements",
    "org.wartremover.warts.Throw"))
  def apply[F[_]: Sync, A, B](
      parserF: F[BaseParser[F, A]])(
      oneChunk: A => Chunk[B],
      manyChunk: List[A] => Chunk[B])
      : Pipe[F, Byte, B] = { s =>

    Stream.eval(parserF) flatMap { parser =>
      val init = s.chunks flatMap { chunk =>
        val chunkF = chunk match {
          case chunk: Chunk.ByteBuffer =>
            covaryErr(parser.absorb(chunk.buf)).rethrow.map(oneChunk)

          case Chunk.ByteVectorChunk(bv) =>
            val manyF = bv.foldLeftBB(Sync[F].delay(new mutable.ListBuffer[A])) { (accF, buf) =>
              accF flatMap { acc =>
                covaryErr(parser.absorb(buf)).rethrow.map(acc += _)
              }
            }

            manyF.map(m => manyChunk(m.toList))

          case chunk =>
            covaryErr(parser.absorb(chunk.toByteBuffer)).rethrow.map(oneChunk)
        }

        Stream.evalUnChunk(chunkF)
      }

      val finishF = covaryErr(parser.finish).rethrow.map(oneChunk)
      init ++ Stream.evalUnChunk(finishF)
    }
  }

  def foldable[F[_]: Sync, G[_]: Foldable, A](parserF: F[BaseParser[F, G[A]]]): Pipe[F, Byte, A] = {
    def oneChunk(ga: G[A]): Chunk[A] =
      Chunk.buffer(ga.foldLeft(new mutable.ArrayBuffer[A])(_ += _))

    def manyChunk(lga: List[G[A]]): Chunk[A] =
      Chunk.buffer(Foldable[List].compose[G].foldLeft(lga, new mutable.ArrayBuffer[A])(_ += _))

    apply(parserF)(oneChunk, manyChunk)
  }

  private[this] def covaryErr[F[_]: Functor, T, A](
      fea: F[Either[T, A]])(
      implicit ev: T As Throwable)
      : F[Either[Throwable, A]] =
    fea.map(_.left.map(ev.coerce(_)))
}
