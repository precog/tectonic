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

import cats.effect.IO

import _root_.fs2.{Chunk, Stream}

import tectonic.json.Parser
import tectonic.test.{Event, ReifiedTerminalPlate}

import org.specs2.mutable.Specification

import scodec.bits.ByteVector

import scala.List

import java.nio.ByteBuffer

object StreamParserSpecs extends Specification {
  import Event._

  val parserF: IO[BaseParser[Chunk[Event]]] =
    IO(Parser(new ReifiedTerminalPlate().mapDelegate(Chunk.seq(_)), Parser.ValueStream))

  "stream parser transduction" should {
    "parse a single value" in {
      val results = Stream.chunk(Chunk.Bytes("42".getBytes)).through(StreamParser(parserF))
      results.compile.toList.unsafeRunSync mustEqual List(Num("42", -1, -1), FinishRow)
    }

    "parse a two values from a single chunk" in {
      val results = Stream.chunk(Chunk.Bytes("42 true".getBytes)).through(StreamParser(parserF))
      val expected = List(Num("42", -1, -1), FinishRow, Tru, FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }

    "parse a value split across two chunks" in {
      val input = Stream.chunk(Chunk.Bytes("4".getBytes)) ++
        Stream.chunk(Chunk.Bytes("2".getBytes))

      val results = input.through(StreamParser(parserF))
      val expected = List(Num("42", -1, -1), FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }

    "parse a two values from two chunks" in {
      val input = Stream.chunk(Chunk.Bytes("42 ".getBytes)) ++
        Stream.chunk(Chunk.Bytes("true".getBytes))

      val results = input.through(StreamParser(parserF))
      val expected = List(Num("42", -1, -1), FinishRow, Tru, FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }

    "parse a value from a bytebuffer chunk" in {
      val input = Stream.chunk(Chunk.ByteBuffer(ByteBuffer.wrap("42".getBytes)))

      val results = input.through(StreamParser(parserF))
      val expected = List(Num("42", -1, -1), FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }

    "parse two values from a split bytevector chunk" in {
      val input = Stream.chunk(
        Chunk.ByteVectorChunk(
          ByteVector.view(ByteBuffer.wrap("42 ".getBytes)) ++
            ByteVector.view(ByteBuffer.wrap("true".getBytes))))

      val results = input.through(StreamParser(parserF))
      val expected = List(Num("42", -1, -1), FinishRow, Tru, FinishRow)

      results.compile.toList.unsafeRunSync mustEqual expected
    }
  }
}
