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
package csv

import org.specs2.mutable.Specification

import tectonic.test.Event
import tectonic.test.csv._

import scala.List

object ParserSpecs extends Specification {
  import Event._

  "excel-style" should {
    implicit val c = Parser.Config()

    "parse a single value in one column" in {
      "abc\r\nfubar\r\n" must parseAs(NestMap("abc"), Str("fubar"), Unnest, FinishRow)
    }

    "parse three values across three columns" in {
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

    "infer headers when unspecified" in {
      implicit val c = Parser.Config().copy(header = false)

      val input = "r1c1,r1c2,r1c3\r\nr2c1,r2c2,r2c3\r\nr3c1,r3c2,r3c3\r\n"
      input must parseAs(
        NestMap("A"), Str("r1c1"), Unnest,
        NestMap("B"), Str("r1c2"), Unnest,
        NestMap("C"), Str("r1c3"), Unnest, FinishRow,
        NestMap("A"), Str("r2c1"), Unnest,
        NestMap("B"), Str("r2c2"), Unnest,
        NestMap("C"), Str("r2c3"), Unnest, FinishRow,
        NestMap("A"), Str("r3c1"), Unnest,
        NestMap("B"), Str("r3c2"), Unnest,
        NestMap("C"), Str("r3c3"), Unnest, FinishRow)
    }

    "infer a really really long header" in {
      import scala.Predef, Predef._

      implicit val c = Parser.Config().copy(header = false)

      val input = (0 until 52).mkString(",") + "\r\n"
      val headers = List(
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        "AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM", "AN", "AO", "AP", "AQ", "AR", "AS", "AT", "AU", "AV", "AW", "AX", "AY", "AZ")

      val generated = headers.zipWithIndex flatMap {
        case (header, i) =>
          List(NestMap(header), Str(i.toString), Unnest)
      }

      (input + input) must parseAs(generated ::: List(FinishRow) ::: generated ::: List(FinishRow): _*)
    }

    "parse a single value with a row ending in EOF" in {
      "abc\r\nfubar" must parseAs(NestMap("abc"), Str("fubar"), Unnest, FinishRow)
    }

    "parse two values with a row ending in EOF" in {
      "abc,def\r\nfubar,baz" must parseAs(
        NestMap("abc"), Str("fubar"), Unnest,
        NestMap("def"), Str("baz"), Unnest, FinishRow)
    }

    "parse a single value with an inferred header ending in EOF" in {
      implicit val c = Parser.Config().copy(header = false)

      "fubar" must parseAs(NestMap("A"), Str("fubar"), Unnest, FinishRow)
    }

    "parse a single quoted value with a row ending in EOF" in {
      "abc\r\n\"fubar\"" must parseAs(NestMap("abc"), Str("fubar"), Unnest, FinishRow)
    }

    "parse a single quoted value with an inferred header ending in EOF" in {
      implicit val c = Parser.Config().copy(header = false)

      "\"fubar\"" must parseAs(NestMap("A"), Str("fubar"), Unnest, FinishRow)
    }

    "deal with doubled row delimiter sequence" in {
      "foo\r\n\r\n" must parseAs(NestMap("foo"), Str(""), Unnest, FinishRow)
    }

    "allow row delimiter sequence in quoted record" in {
      "a\r\n\"fu\r\nbar\"\r\n" must parseAs(NestMap("a"), Str("fu\r\nbar"), Unnest, FinishRow)
    }
  }

  "excel-style with unix newlines" should {
    implicit val c = Parser.Config().copy(row1 = '\n', row2 = 0)

    "parse a single value" in {
      "abc\nfubar\n" must parseAs(NestMap("abc"), Str("fubar"), Unnest, FinishRow)
    }

    "parse three values across three columns" in {
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

    "infer headers when unspecified" in {
      implicit val c = Parser.Config().copy(header = false, row1 = '\n', row2 = 0)

      val input = "r1c1,r1c2,r1c3\nr2c1,r2c2,r2c3\nr3c1,r3c2,r3c3\n"
      input must parseAs(
        NestMap("A"), Str("r1c1"), Unnest,
        NestMap("B"), Str("r1c2"), Unnest,
        NestMap("C"), Str("r1c3"), Unnest, FinishRow,
        NestMap("A"), Str("r2c1"), Unnest,
        NestMap("B"), Str("r2c2"), Unnest,
        NestMap("C"), Str("r2c3"), Unnest, FinishRow,
        NestMap("A"), Str("r3c1"), Unnest,
        NestMap("B"), Str("r3c2"), Unnest,
        NestMap("C"), Str("r3c3"), Unnest, FinishRow)
    }
  }

  "error reporting" should {
    implicit val c = Parser.Config(row1 = '\n', row2 = 0)

    "disallow empty headers" in {
      "abc,,def" must failParseWithError {
        case ParseException(msg, _, _, _) =>
          msg must startWith("empty header cell")
      }
    }

    "disallow empty quoted headers" in {
      "abc,\"\",def" must failParseWithError {
        case ParseException(msg, _, _, _) =>
          msg must startWith("empty header cell")
      }
    }

    "catch EOF in header" in {
      "abc,def" must failParseWithError {
        case ParseException(msg, _, _, _) =>
          msg must startWith("unexpected end of file in header row")
      }
    }

    "catch missing records at EOF" in {
      "abc,def\nfoo" must failParseWithError {
        case ParseException(msg, _, _, _) =>
          msg must startWith("unexpected end of file: missing records")
      }
    }

    "detect unexpected trailing characters" in {
      "abc\n\"def\"derp\n" must failParseWithError {
        case ParseException(msg, _, _, _) =>
          msg must startWith("unexpected character found at record boundary")
      }
    }

    "detect unclosed quoting" in {
      "foo\n\"abc" must failParseWithError {
        case ParseException(msg, _, _, _) =>
          msg must startWith("unexpected end of file: unclosed quoted record")
      }
    }

    "detect unclosed quoting after escaped close" in {
      "abc\n\"def\"\"derp\n" must failParseWithError {
        case ParseException(msg, _, _, _) =>
          msg must startWith("unexpected end of file: unclosed quoted record")
      }
    }
  }
}
