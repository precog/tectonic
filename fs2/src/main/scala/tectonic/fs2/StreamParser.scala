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
package fs2

import cats.effect.Sync
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
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def apply[F[_]: Sync, A](
      parserF: F[BaseParser[Chunk[A]]])
      : Pipe[F, Byte, A] = { s =>

    Stream.eval(parserF) flatMap { parser =>
      val init = s.chunks flatMap { chunk =>
        val listF = chunk match {
          case chunk: Chunk.ByteBuffer =>
            Sync[F].delay(parser.absorb(chunk.buf): Either[Throwable, Chunk[A]]).rethrow.map(List(_))

          case Chunk.ByteVectorChunk(bv) =>
            Sync[F] delay {
              bv.foldLeftBB(List[Chunk[A]]()) { (acc, buf) =>
                parser.absorb(buf).fold(throw _, _ :: acc)
              }
            }

          case chunk =>
            Sync[F].delay(parser.absorb(chunk.toByteBuffer): Either[Throwable, Chunk[A]]).rethrow.map(List(_))
        }

        val chunkF = listF map { cs =>
          val buffer = new mutable.ListBuffer[A]

          cs.foldRight(()) { (c, _) =>
            c.foldLeft(()) { (_, a) =>
              val _ = buffer += a
              ()
            }
          }

          Chunk.seq(buffer.toList)
        }

        Stream.evalUnChunk(chunkF)
      }

      val finishF = Sync[F].delay(parser.finish(): Either[Throwable, Chunk[A]]).rethrow map { ca =>
        val buffer = new mutable.ListBuffer[A]
        ca.foldLeft(()) { (_, a) =>
          val _ = buffer += a
          ()
        }
        Chunk.seq(buffer.toList)
      }

      init ++ Stream.evalUnChunk(finishF)
    }
  }
}
