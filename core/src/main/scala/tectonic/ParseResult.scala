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

import cats.{Eval, Eq, Foldable, MonadError, Monoid, Semigroup}
import cats.implicits._

import scala.{Int, Nothing, Product, Serializable, StringContext}
import scala.annotation.tailrec
import scala.util.{Either, Left, Right}

sealed trait ParseResult[+A] extends Product with Serializable {

  def toEitherComplete: Either[ParseException, A] =
    this match {
      case ParseResult.Failure(e) =>
        Left(e)

      case ParseResult.Partial(a, remaining) =>
        Left(ParseException(s"partial parse success producing value $a with $remaining bytes remaining", -1, -1, -1))

      case ParseResult.Complete(a) =>
        Right(a)
    }
}

private[tectonic] sealed trait LowPriorityInstances {
  import ParseResult.{Complete, Partial, Failure}

  implicit def semigroup[A: Semigroup]: Semigroup[ParseResult[A]] =
    new ParseResultSemigroup[A]

  protected class ParseResultSemigroup[A: Semigroup] extends Semigroup[ParseResult[A]] {
    def combine(left: ParseResult[A], right: ParseResult[A]) =
      (left, right) match {
        case (f @ Failure(_), _) => f
        case (_, f @ Failure(_)) => f
        case (Partial(a1, rem1), Partial(a2, rem2)) => Partial(a1 |+| a2, rem1 + rem2)
        case (Partial(a1, rem), Complete(a2)) => Partial(a1 |+| a2, rem)
        case (Complete(a2), Partial(a1, rem)) => Partial(a1 |+| a2, rem)
        case (Complete(a1), Complete(a2)) => Complete(a1 |+| a2)
      }
  }
}

object ParseResult extends LowPriorityInstances {

  implicit def eq[A: Eq]: Eq[ParseResult[A]] =
    Eq instance {
      case (Failure(e1), Failure(e2)) => e1 == e2
      case (Partial(a1, rem1), Partial(a2, rem2)) => a1 === a2 && rem1 === rem2
      case (Complete(a1), Complete(a2)) => a1 === a2
      case _ => false
    }

  implicit def monoid[A: Monoid]: Monoid[ParseResult[A]] =
    new ParseResultSemigroup[A] with Monoid[ParseResult[A]]{
      def empty = Complete(Monoid[A].empty)
    }

  implicit val instances: MonadError[ParseResult, ParseException] with Foldable[ParseResult] =
    new MonadError[ParseResult, ParseException] with Foldable[ParseResult] {

      def pure[A](a: A): ParseResult[A] = Complete(a)

      def handleErrorWith[A](fa: ParseResult[A])(f: ParseException => ParseResult[A]): ParseResult[A] =
        fa match {
          case Failure(e) => f(e)
          case other => other
        }

      def raiseError[A](e: ParseException): ParseResult[A] = Failure(e)

      def flatMap[A, B](fa: ParseResult[A])(f: A => ParseResult[B]): ParseResult[B] =
        fa match {
          case Failure(e) => Failure(e)

          case Partial(a, rem1) => f(a) match {
            case Failure(e) => Failure(e)
            case Partial(b, rem2) => Partial(b, rem1 + rem2)
            case Complete(b) => Partial(b, rem1)
          }

          case Complete(a) => f(a)
        }

      def tailRecM[A, B](a: A)(f: A => ParseResult[Either[A, B]]): ParseResult[B] = {
        @tailrec
        def loop(a: A, rem: Int): ParseResult[B] =
          f(a) match {
            case Failure(e) => Failure(e)
            case Partial(Left(a), rem2) => loop(a, rem + rem2)
            case Partial(Right(b), rem2) => Partial(b, rem + rem2)
            case Complete(Left(a)) => loop(a, rem)

            case Complete(Right(b)) =>
              if (rem > 0)
                Partial(b, rem)
              else
                Complete(b)
          }

        loop(a, 0)
      }

      def foldLeft[A, B](fa: ParseResult[A], b: B)(f: (B, A) => B): B =
        fa match {
          case Failure(_) => b
          case Partial(a, _) => f(b, a)
          case Complete(a) => f(b, a)
        }

      def foldRight[A, B](fa: ParseResult[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        fa match {
          case Failure(_) => lb
          case Partial(a, _) => f(a, lb)
          case Complete(a) => f(a, lb)
        }
    }

  final case class Failure(e: ParseException) extends ParseResult[Nothing]
  final case class Partial[+A](a: A, remaining: Int) extends ParseResult[A]
  final case class Complete[+A](a: A) extends ParseResult[A]
}
