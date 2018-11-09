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

import scala.{inline, Array, Boolean, Byte, Char, Int, Nothing, Unit}
import scala.annotation.{switch, tailrec}
import scala.util.{Either, Left, Right}

import java.lang.{CharSequence, String, SuppressWarnings, System}

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Var",
      "org.wartremover.warts.FinalVal",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.PublicInference"))
final class Parser[A](plate: Plate[A], config: Parser.Config) extends BaseParser[A] {

  /*
   * The state is a three-bit value representing the parsing of
   * a record or a delimiter in all three of the following scenarios:
   *
   * - Normal row
   * - Header row
   * - Normal row that is *first* in a headerless input (infererence)
   *
   * Thus, INFERRING and HEADER are mutually exclusive, but we can't
   * represent that directly in the bits.
   */

  @inline private[this] final val BASE_MASK = 1
  @inline private[this] final val HEADER_MASK = 1 << 1
  @inline private[this] final val INFER_MASK = 1 << 2

  @inline private[this] final val RECORD = 0       // awaiting a record
  @inline private[this] final val END = 1          // awaiting a delimiter

  @inline private[this] final val ROW = 0          // parsing a row
  @inline private[this] final val HEADER = 1 << 1  // parsing a header

  @inline private[this] final val NOT_INFERRING = 0   // not inferring headers
  @inline private[this] final val INFERRING = 1 << 2  // inferring headers

  private[this] final var state = if (config.header) HEADER else INFERRING

  private[this] final var column = 0    // current column, regardless of state
  private[this] final var header: Array[CharSequence] = new Array[CharSequence](32)   // hopefully optimize for 32 columns
  private[this] final var headerMax = -1

  private[this] final val record: Int = config.record & 0xff
  private[this] final val row1: Int = config.row1 & 0xff
  private[this] final val row2: Int = config.row2 & 0xff
  private[this] final val openQuote: Int = config.openQuote & 0xff
  private[this] final val closeQuote: Int = config.closeQuote & 0xff
  private[this] final val escape: Int = config.escape & 0xff

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.NonUnitStatements"))
  protected[this] final def churn(): Either[ParseException, A] = {
    try {
      parse(state, curr, column)
    } catch {
      case AsyncException =>
        if (done) {
          // if we are done, make sure we ended at a good stopping point
          if ((state & HEADER) == 0) {
            (state & BASE_MASK: @switch) match {
              case RECORD =>
                if (column == 0) {
                  // we just saw a complete row, just finish up
                  Right(plate.finishBatch(true))
                } else if (atEndOfRow(column) || (state & INFER_MASK) == INFERRING) {
                  plate.nestMap(header(column))
                  plate.str("")
                  plate.unnest()
                  plate.finishRow()
                  Right(plate.finishBatch(true))
                } else {
                  Left(ParseException("unexpected end of file: missing records", -1, -1, -1))
                }

              case END =>
                // treat EOF at the end of the row as an implicit record delimiter
                if (atEndOfRow(column) || (state & INFER_MASK) == INFERRING) {
                  plate.finishRow()
                  Right(plate.finishBatch(true))
                } else {
                  Left(ParseException("unexpected end of file: missing records", -1, -1, -1))
                }
            }
          } else {
            Left(ParseException("unexpected end of file in header row", -1, -1, -1))
          }
        } else {
          // we ran out of data, so return what we have so far
          Right(plate.finishBatch(false))
        }

      case e: ParseException =>
        // we hit a parser error, so return that error and results so far
        Left(e)
    }
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Return",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Throw"))
  private[this] final def parseRecordUnquoted(i: Int): CharSequence = {
    if (done) {
      return parseRecordUnquotedDone(i)
    }

    var j = i
    var c: Int = byte(j) & 0xff
    while (true) {
      if (c == record) {
        this.curr = j
        return at(i, j)
      } else if (c == row1 && (row2 == 0 || (byte(j + 1) & 0xff) == row2)) {
        this.curr = j
        return at(i, j)
      }

      j += 1
      c = byte(j) & 0xff
    }

    error("impossible")
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Return",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Throw"))
  private[this] final def parseRecordUnquotedDone(i: Int): CharSequence = {
    val data = unsafeData()
    val len = unsafeLen()
    var j = i
    while (j < len) {
      val c: Int = data(j) & 0xff

      if (c == record) {
        this.curr = j
        return at(i, j)
      } else if (c == row1 && (row2 == 0 || (j + 1 < len && (data(j + 1) & 0xff) == row2))) {
        this.curr = j
        return at(i, j)
      }

      j += 1
    }

    this.curr = j
    at(i, j)
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Return",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Throw"))
  private[this] final def parseRecordQuoted(i: Int): CharSequence = {
    if (done) {
      return parseRecordQuotedDone(i)
    }

    val sb = new CharBuilder

    var j = i
    var c: Int = byte(j) & 0xff
    while (true) {
      if (c == escape && (byte(j + 1) & 0xff) == closeQuote) {
        sb.append(closeQuote.toChar)
        j += 2
      } else {
        if (c == closeQuote) {
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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Return",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Throw"))
  private[this] final def parseRecordQuotedDone(i: Int): CharSequence = {
    val data = unsafeData()
    val len = unsafeLen()
    val sb = new CharBuilder

    var j = i
    while (j < len) {
      val c: Int = data(j) & 0xff

      if (c == escape && j + 1 < len && (data(j + 1) & 0xff) == closeQuote) {
        sb.append(closeQuote.toChar)
        j += 2
      } else {
        if (c == closeQuote) {
          this.curr = j + 1
          return sb.makeString
        } else if (c < 128) {
          // 1-byte UTF-8 sequence
          sb.append(c.toChar)
          j += 1
        } else if ((c & 224) == 192) {
          // 2-byte UTF-8 sequence
          if (j + 1 < len) {
            sb.extend(at(j, j + 2))
            j += 2
          } else {
            die(j, "unexpected end of file: unclosed quoted record")
          }
        } else if ((c & 240) == 224) {
          // 3-byte UTF-8 sequence
          if (j + 2 < len) {
            sb.extend(at(j, j + 3))
            j += 3
          } else {
            die(j, "unexpected end of file: unclosed quoted record")
          }
        } else if ((c & 248) == 240) {
          // 4-byte UTF-8 sequence
          if (j + 3 < len) {
            sb.extend(at(j, j + 4))
            j += 4
          } else {
            die(j, "unexpected end of file: unclosed quoted record")
          }
        } else {
          die(j, "invalid UTF-8 encoding")
        }
      }
    }

    die(j - 1, "unexpected end of file: unclosed quoted record")
  }

  @tailrec
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Throw",
      "org.wartremover.warts.Throw"))
  private[this] final def parse(state: Int, j: Int, column: Int): Nothing = {
    val i = reset(j)

    this.state = state
    this.curr = i
    this.column = column

    val inHeader = (state & HEADER_MASK) == HEADER
    val inferring = (state & INFER_MASK) == INFERRING

    if (inHeader || inferring) {
      checkHeader(column)
    }

    // remove the base
    val continue = state & ~BASE_MASK

    (state & BASE_MASK: @switch) match {
      case RECORD =>
        val c: Int = byte(i) & 0xff

        if (c == record) {
          if (inHeader) {
            die(i, "empty header cell")
          }

          if (inferring) {
            header(column) = asHeader(column)
          }

          plate.nestMap(header(column))
          plate.str("")
          plate.unnest()

          parse(continue | RECORD, i + 1, column + 1)
        } else if (c == row1) {
          if (row2 == 0) {
            if (inHeader) {
              die(i, "empty header cell")
            }

            if (inferring) {
              header(column) = asHeader(column)
              this.headerMax = column
            }

            if (inferring || atEndOfRow(column)) {
              plate.nestMap(header(column))
              plate.str("")
              plate.unnest()
              plate.finishRow()

              parse(RECORD, i + 1, 0)
            } else {
              die(i, "unexpected end of row: missing records")
            }
          } else if ((byte(i + 1) & 0xff) == row2) {
            if (inHeader) {
              die(i, "empty header cell")
            }

            if (inferring) {
              header(column) = asHeader(column)
              this.headerMax = column
            }

            if (inferring || atEndOfRow(column)) {
              plate.nestMap(header(column))
              plate.str("")
              plate.unnest()
              plate.finishRow()

              parse(RECORD, i + 2, 0)
            } else {
              die(i, "unexpected end of row: missing records")
            }
          } else {
            val str = parseRecordUnquoted(i)    // consume first, so we get the AsyncException if relevant

            if (inHeader) {
              header(column) = str
            } else {
              if (inferring) {
                header(column) = asHeader(column)
              }

              plate.nestMap(header(column))
              plate.str(str)
              plate.unnest()
            }

            parse(continue | END, curr, column)
          }
        } else if (c == openQuote) {
          val str = parseRecordQuoted(i + 1)    // consume first, so we get the AsyncException if relevant

          if (inHeader) {
            if (str.length == 0) {
              die(i, "empty header cell")
            }

            header(column) = str
          } else {
            if (inferring) {
              header(column) = asHeader(column)
            }

            plate.nestMap(header(column))
            plate.str(str)
            plate.unnest()
          }

          parse(continue | END, curr, column)
        } else {
          val str = parseRecordUnquoted(i)    // consume first, so we get the AsyncException if relevant

          if (inHeader) {
            header(column) = str
          } else {
            if (inferring) {
              header(column) = asHeader(column)
            }

            plate.nestMap(header(column))
            plate.str(str)
            plate.unnest()
          }

          parse(continue | END, curr, column)
        }

      case END =>
        val c: Int = byte(i) & 0xff

        if (c == record) {
          parse(continue | RECORD, i + 1, column + 1)
        } else if (c == row1) {
          if (row2 == 0) {
            if (inferring || inHeader) {
              this.headerMax = column
            }

            if (!inHeader) {
              plate.finishRow()
            }

            parse(RECORD, i + 1, 0)
          } else if ((byte(i + 1) & 0xff) == row2) {
            if (inferring || inHeader) {
              this.headerMax = column
            }

            if (!inHeader) {
              plate.finishRow()
            }

            parse(RECORD, i + 2, 0)
          } else {
            die(i, "unexpected character found at record boundary")
          }
        } else {
          die(i, "unexpected character found at record boundary")
        }

      case _ => error("impossible state in parse: " + state.toString)
    }
  }

  private[this] final def checkHeader(column: Int): Unit = {
    if (column >= header.length) {
      val old = header
      header = new Array[CharSequence](old.length * 2)
      System.arraycopy(header, 0, old, 0, old.length)
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

    if (back.length > 1) {
      back(0) = (back(0) - 1).toChar    // this is stupid, but we want A = 1 in the most-significant column
    }

    new String(back)
  }

  @inline
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private[this] final def atEndOfRow(column: Int): Boolean =
    column == headerMax
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
