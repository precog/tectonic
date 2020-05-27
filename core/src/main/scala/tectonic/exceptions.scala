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

import scala.Int
import scala.util.control

import java.lang.{Exception, String}

final case class ParseException(msg: String, index: Int, line: Int, col: Int) extends Exception(msg)
final case class IncompleteParseException(msg: String) extends Exception(msg)

/**
 * This class is used internally by Parser to signal that we've
 * reached the end of the particular input we were given.
 */
private[tectonic] case object AsyncException extends Exception with control.NoStackTrace

/**
 * This class is used to signal that we *haven't* reached the end of
 * our particular input, but the underlying plate is reaching the end
 * of their reasonable buffer space and is in need of a lazy page
 * boundary upstream.
 */
private[tectonic] case object PartialBatchException extends Exception with control.NoStackTrace

/**
 * This is a more prosaic exception which indicates that we've hit a
 * parsing error.
 */
private[tectonic] final class FailureException extends Exception
