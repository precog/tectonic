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
package csv

import org.specs2.mutable.Specification

import tectonic.test.Event
import tectonic.test.csv._

object ParserSpecs extends Specification {
  import Event._

  "excel-style" should {
    implicit val c = Parser.Config()

    "parse a single value" in {
      "abc\r\nfubar\r\n" must parseAs(NestMap("abc"), Str("fubar"), Unnest, FinishRow)
    }

    "parse three values across two columns" in {
      val input = "a,b,c\r\nr1c1,r1c2,r1c3\r\nr2c1,r2c2,r2c3\r\nr3c1,r3c2,r3c3\r\n"
      input must parseAs(
        NestMap("a"), Str("r1c1"), Unnest,
        NestMap("b"), Str("r1c2"), Unnest,
        NestMap("c"), Str("r1c3"), Unnest, FinishRow,
        NestMap("a"), Str("r2c1"), Unnest,
        NestMap("b"), Str("r2c2"), Unnest,
        NestMap("c"), Str("r2c3"), Unnest, FinishRow,
        NestMap("a"), Str("r3c1"), Unnest,
        NestMap("b"), Str("r3c2"), Unnest,
        NestMap("c"), Str("r3c3"), Unnest, FinishRow)
    }

    "allow \\r in values" in {
      "a\r\nfu\rbar\r\n" must parseAs(NestMap("a"), Str("fu\rbar"), Unnest, FinishRow)
    }

    "allow , in quoted values" in {
      "a\r\n\"fu,bar\"\r\n" must parseAs(NestMap("a"), Str("fu,bar"), Unnest, FinishRow)
    }

    "consume record delimiter following a quoted value" in {
      "a,b\r\n\"fu,bar\",baz\r\n" must parseAs(
        NestMap("a"), Str("fu,bar"), Unnest,
        NestMap("b"), Str("baz"), Unnest, FinishRow)
    }

    "allow \" in quoted values with escaping" in {
      "a\r\n\"fu\"\"bar\"\r\n" must parseAs(NestMap("a"), Str("fu\"bar"), Unnest, FinishRow)
    }
  }

  "excel-style with unix newlines" should {
    implicit val c = Parser.Config().copy(row1 = '\n', row2 = 0)

    "parse a single value" in {
      "abc\nfubar\n" must parseAs(NestMap("abc"), Str("fubar"), Unnest, FinishRow)
    }

    "parse three values across two columns" in {
      val input = "a,b,c\nr1c1,r1c2,r1c3\nr2c1,r2c2,r2c3\nr3c1,r3c2,r3c3\n"
      input must parseAs(
        NestMap("a"), Str("r1c1"), Unnest,
        NestMap("b"), Str("r1c2"), Unnest,
        NestMap("c"), Str("r1c3"), Unnest, FinishRow,
        NestMap("a"), Str("r2c1"), Unnest,
        NestMap("b"), Str("r2c2"), Unnest,
        NestMap("c"), Str("r2c3"), Unnest, FinishRow,
        NestMap("a"), Str("r3c1"), Unnest,
        NestMap("b"), Str("r3c2"), Unnest,
        NestMap("c"), Str("r3c3"), Unnest, FinishRow)
    }
  }
}
