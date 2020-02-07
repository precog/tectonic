/*
 * Copyright 2014–2020 SlamData Inc.
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
package util

import scala.{Boolean, Product, Serializable}

private[tectonic] sealed abstract class BList extends Product with Serializable {
  def head: Boolean
  def ::(head: Boolean): BList = BList.Cons(head, this)
}

private[tectonic] object BList {
  final case class Cons(head: Boolean, tail: BList) extends BList
  final case class Last(head: Boolean) extends BList
}
