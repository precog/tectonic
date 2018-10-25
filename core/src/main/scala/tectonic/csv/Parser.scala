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

import tectonic.util.CharBuilder

import scala.{inline, Array, Boolean, Byte, Char, Int, List, Nil, Nothing}
import scala.annotation.{switch, tailrec}
import scala.util.{Either, Left, Right}

import java.lang.{CharSequence, String, SuppressWarnings}

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Var",
      "org.wartremover.warts.FinalVal",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.PublicInference"))
final class Parser[A](plate: Plate[A], config: Parser.Config) extends BaseParser[A] {

  @inline private[this] final val SIG_RECORD = 1
  @inline private[this] final val SIG_ROW1 = 2
  @inline private[this] final val SIG_OPEN_QUOTE = 4

  @inline private[this] final val DATA = 0       // awaiting a record
  @inline private[this] final val HEADER = 1       // awaiting a header entry
  @inline private[this] final val IHEADER = 2       // awaiting an inferred header entry

  private[this] var state = if (config.header) HEADER else IHEADER
  private[this] var pheader: List[CharSequence] = Nil   // parsed header entries in reverse order

  private[this] var column = 0    // current column, regardless of state
  private[this] var header: Array[CharSequence] = _   // actual header

  private[this] final val record: Int = config.record & 0xff
  private[this] final val row1: Int = config.row1 & 0xff
  private[this] final val row2: Int = config.row2 & 0xff
  private[this] final val openQuote: Int = config.openQuote & 0xff
  private[this] final val closeQuote: Int = config.closeQuote & 0xff
  private[this] final val escape: Int = config.escape & 0xff

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  protected[this] final def churn(): Either[ParseException, A] = {
    try {
      (state: @switch) match {
        case DATA =>
          parse(curr, column)

        case HEADER =>
          hparse(curr, column, pheader)

        case IHEADER =>
          hiparse(curr, column, pheader)

        case _ =>
          error("invalid state in churn: " + state.toString)
      }
    } catch {
      case e: AsyncException =>
        if (done) {
          // if we are done, make sure we ended at a good stopping point
          if ((state == DATA || state == IHEADER) && column == 0)
            Right(plate.finishBatch(true))
          else
            Left(ParseException("exhausted input", -1, -1, -1))
        } else {
          // we ran out of data, so return what we have so far
          Right(plate.finishBatch(false))
        }

      case e: ParseException =>
        // we hit a parser error, so return that error and results so far
        Left(e)
    }
  }

  // must consume record delimiter; must NOT consume row delimiter
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Return",
      "org.wartremover.warts.While"))
  private[this] final def parseRecordUnquoted(i: Int): CharSequence = {
    var j = i
    var c: Int = byte(j) & 0xff
    while (true) {
      if (c == record) {
        this.curr = j + 1   // consume the record delimiter
        return at(i, j)
      } else if (c == row1 && (row2 == '\u0000' || (byte(j + 1) & 0xff) == row2)) {
        this.curr = j   // don't consume the row delimiter
        return at(i, j)
      }

      j += 1
      c = byte(j) & 0xff
    }

    error("impossible")
  }

  // must consume record delimiter; must NOT consume row delimiter
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Return",
      "org.wartremover.warts.While"))
  private[this] final def parseRecordQuoted(i: Int): CharSequence = {
    val sb = new CharBuilder

    var j = i
    var c: Int = byte(j) & 0xff
    while (true) {
      if (c == escape && (byte(j + 1) & 0xff) == closeQuote) {
        sb.append(closeQuote.toChar)
        j += 2
      } else {
        if (c == closeQuote) {
          if ((byte(j + 1) & 0xff) == record) {
            j += 1
          }
          this.curr = j + 1
          return sb.makeString
        } else if (c < 128) {
          // 1-byte UTF-8 sequence
          sb.append(c.toChar)
          j += 1
        } else if ((c & 224) == 192) {
          // 2-byte UTF-8 sequence
          sb.extend(at(j, j + 2))
          j += 2
        } else if ((c & 240) == 224) {
          // 3-byte UTF-8 sequence
          sb.extend(at(j, j + 3))
          j += 3
        } else if ((c & 248) == 240) {
          // 4-byte UTF-8 sequence
          sb.extend(at(j, j + 4))
          j += 4
        } else {
          die(j, "invalid UTF-8 encoding")
        }
      }

      c = byte(j) & 0xff
    }

    error("impossible")
  }

  @tailrec
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Equals"))
  private[this] final def parse(j: Int, column: Int): Nothing = {
    val i = reset(j)

    this.state = DATA
    this.curr = i
    this.column = column

    val c: Int = byte(i) & 0xff

    if (c == record) {
      plate.nestMap(header(column))
      plate.str("")
      plate.unnest()

      parse(i + 1, column + 1)
    } else if (c == row1) {
      if (row2 == '\u0000') {
        plate.finishRow()
        parse(i + 1, 0)
      } else if ((byte(i + 1) & 0xff) == row2) {
        plate.finishRow()
        parse(i + 2, 0)
      } else {
        val str = parseRecordUnquoted(i)    // consume first, so we get the AsyncException if relevant
        plate.nestMap(header(column))
        plate.str(str)
        plate.unnest()
        parse(curr, column + 1)
      }
    } else if (c == openQuote) {
      val str = parseRecordQuoted(i + 1)    // consume first, so we get the AsyncException if relevant
      plate.nestMap(header(column))
      plate.str(str)
      plate.unnest()
      parse(curr, column + 1)
    } else {
      val str = parseRecordUnquoted(i)    // consume first, so we get the AsyncException if relevant
      plate.nestMap(header(column))
      plate.str(str)
      plate.unnest()
      parse(curr, column + 1)
    }
  }

  @tailrec
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Equals"))
  private[this] final def hparse(j: Int, column: Int, pheader: List[CharSequence]): Nothing = {
    val i = reset(j)

    this.curr = i
    this.column = column
    this.pheader = pheader

    val c: Int = byte(i) & 0xff

    if (c == record) {
      die(offset, "header field cannot be empty")
    } else if (c == row1) {
      if (row2 == '\u0000') {
        this.header = pheader.reverse.toArray
        parse(i + 1, 0)
      } else if ((byte(i + 1) & 0xff) == row2) {
        this.header = pheader.reverse.toArray
        parse(i + 2, 0)
      } else {
        val str = parseRecordUnquoted(i)    // consume first, so we get the AsyncException if relevant
        hparse(curr, column + 1, str :: pheader)
      }
    } else if (c == openQuote) {
      val str = parseRecordQuoted(i + 1)    // consume first, so we get the AsyncException if relevant
      hparse(curr, column + 1, str :: pheader)
    } else {
      val str = parseRecordUnquoted(i)    // consume first, so we get the AsyncException if relevant
      hparse(curr, column + 1, str :: pheader)
    }
  }

  @tailrec
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Equals"))
  private[this] final def hiparse(j: Int, column: Int, pheader: List[CharSequence]): Nothing = {
    val i = reset(j)

    this.curr = i
    this.column = column
    this.pheader = pheader

    val c: Int = byte(i) & 0xff

    if (c == record) {
      val curh = asHeader(column)

      plate.nestMap(curh)
      plate.str("")
      plate.unnest()

      hiparse(i + 1, column + 1, curh :: pheader)
    } else if (c == row1) {
      if (row2 == '\u0000') {
        this.header = pheader.reverse.toArray
        plate.finishRow()
        parse(i + 1, 0)
      } else if ((byte(i + 1) & 0xff) == row2) {
        this.header = pheader.reverse.toArray
        parse(i + 2, 0)
      } else {
        val curh = asHeader(column)
        val str = parseRecordUnquoted(i)    // consume first, so we get the AsyncException if relevant
        plate.nestMap(curh)
        plate.str(str)
        plate.unnest()
        hiparse(curr, column + 1, curh :: pheader)
      }
    } else if (c == openQuote) {
      val curh = asHeader(column)
      val str = parseRecordQuoted(i + 1)    // consume first, so we get the AsyncException if relevant
      plate.nestMap(curh)
      plate.str(str)
      plate.unnest()
      hiparse(curr, column + 1, curh :: pheader)
    } else {
      val curh = asHeader(column)
      val str = parseRecordUnquoted(i)    // consume first, so we get the AsyncException if relevant
      plate.nestMap(curh)
      plate.str(str)
      plate.unnest()
      hiparse(curr, column + 1, curh :: pheader)
    }
  }

  // generates things like A, B, C, ..., Z, AA, AB, etc...
  // literally converts column into big-endian base-26
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private[this] final def asHeader(column: Int): String = {
    val back = new Array[Char](column / 26 + 1)    // deal with ceiling
    var i = 0
    var cur = column
    do {
      back((back.length - 1) - i) = ((cur % 26) + 'A').toChar
      i += 1
      cur /= 26
    } while (cur > 0)

    new String(back, 0, back.length - i)    // we might have gone 1 too far
  }
}

object Parser {

  def apply[A](plate: Plate[A], config: Config): Parser[A] =
    new Parser[A](plate, config)

  // defaults to Excel-style with Windows newlines
  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  final case class Config(
      header: Boolean = true,
      record: Byte = ',',
      row1: Byte = '\r',
      row2: Byte = '\n',    // if this is unneeded, it should be set to \0
      openQuote: Byte = '"',    // e.g. “
      closeQuote: Byte = '"',   // e.g. ”
      escape: Byte = '"')   // e.g. \\
}
