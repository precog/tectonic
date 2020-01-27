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
package fs2

import cats.effect.IO
import cats.instances.list._

import _root_.fs2.{Chunk, Pipe, Stream}

import tectonic.json.Parser
import tectonic.test.{Event, ReifiedTerminalPlate}

import org.specs2.mutable.Specification

import scodec.bits.ByteVector

import scala.{Byte, List}

import java.nio.ByteBuffer

class StreamParserSpecs extends Specification {
  import Event._

  val parserF: IO[BaseParser[IO, List[Event]]] =
    Parser(ReifiedTerminalPlate[IO](), Parser.ValueStream)

  val parser: Pipe[IO, Byte, Event] =
    StreamParser.foldable(parserF)

  "stream parser transduction" should {
    "parse a single value" in {
      val results = Stream.chunk(Chunk.Bytes("42".getBytes)).through(parser)
      results.compile.toList.unsafeRunSync mustEqual List(Num("42", -1, -1), FinishRow)
    }

    "parse two values from a single chunk" in {
      val results = Stream.chunk(Chunk.Bytes("16 true".getBytes)).through(parser)
      val expected = List(Num("16", -1, -1), FinishRow, Tru, FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }

    "parse a value split across two chunks" in {
      val input = Stream.chunk(Chunk.Bytes("7".getBytes)) ++
        Stream.chunk(Chunk.Bytes("9".getBytes))

      val results = input.through(parser)
      val expected = List(Num("79", -1, -1), FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }

    "parse two values from two chunks" in {
      val input = Stream.chunk(Chunk.Bytes("321 ".getBytes)) ++
        Stream.chunk(Chunk.Bytes("true".getBytes))

      val results = input.through(parser)
      val expected = List(Num("321", -1, -1), FinishRow, Tru, FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }

    "parse a value from a bytebuffer chunk" in {
      val input = Stream.chunk(Chunk.ByteBuffer(ByteBuffer.wrap("123".getBytes)))

      val results = input.through(parser)
      val expected = List(Num("123", -1, -1), FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }

    "parse two values from a split bytevector chunk" in {
      val input = Stream.chunk(
        Chunk.ByteVectorChunk(
          ByteVector.view(ByteBuffer.wrap("456 ".getBytes)) ++
            ByteVector.view(ByteBuffer.wrap("true".getBytes))))

      val results = input.through(parser)
      val expected = List(Num("456", -1, -1), FinishRow, Tru, FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }
  }
}
