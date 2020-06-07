/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ing.wbaa.druid.dql
// scalastyle:off
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.expressions._

import scala.language.implicitConversions

object DSL
    extends FilteringExpressionOps
    with ExtractionFnOps
    with AggregationOps
    with PostAggregationOps {

  /**
    * @return a new instance of the QueryBuilder
    */
  def DQL: QueryBuilder = new QueryBuilder

  /**
    * Implicitly convert a Scala Symbol to an instance of Dim
    */
  implicit def symbolToDim(s: Symbol): Dim = new Dim(s.name)

  implicit def symbolsToDims[T <: Iterable[Symbol]](iterable: T): Iterable[Dim] =
    iterable.map(s => new Dim(s.name))

  implicit class StringToDim(val sc: StringContext) extends AnyVal {

    /**
      * Create a dim using simple string interpolator.
      *
      * {{{
      *   val prefix = "foo"
      *   val suffix = "bar"
      *
      *   d"${prefix}_${suffix}"
      * }}}
      *
      * @param args The arguments to be inserted into the resulting string.
      *
      * @see StringContext
      */
    def d(args: Any*): Dim = Dim(sc.s(args: _*))
  }

  /**
    * Create an instance of Dim
    * @param name the name of the dimension
    *
    * @return the resulting Dim
    */
  def dim(name: String): Dim = Dim(name)

  implicit class StringOps(val value: String) extends AnyVal {
    def ===(s: Dim): BaseExpression = s === value
    def =!=(s: Dim): BaseExpression = s =!= value
  }

  implicit class NumOps(val value: Double) extends AnyVal {

    @inline
    private def arithmeticPostAgg(s: Dim, fn: ArithmeticFunction): ArithmeticPostAgg =
      ArithmeticPostAgg(
        new ConstantPostAgg(value),
        new FieldAccessPostAgg(s.name),
        fn = fn
      )

    def ===(s: Dim): BaseExpression = s === value
    def =!=(s: Dim): BaseExpression = s =!= value
    def >(s: Dim): BaseExpression   = s < value
    def >=(s: Dim): BaseExpression  = s <= value
    def <(s: Dim): BaseExpression   = s > value
    def <=(s: Dim): BaseExpression  = s >= value

    //def +(s: Dim): ArithmeticPostAgg = arithmeticPostAgg(s, ArithmeticFunction.PLUS)
    def +(s: Dim): BaseArithmeticExpression = BaseArithmeticExpression(
      portAgg = arithmeticPostAgg(s, ArithmeticFunction.PLUS),
      expression = ExpressionOps.add(Expr(value), Expr(s.getName))
    )
    //def -(s: Dim): ArithmeticPostAgg = arithmeticPostAgg(s, ArithmeticFunction.MINUS)
    def -(s: Dim): BaseArithmeticExpression = BaseArithmeticExpression(
      portAgg = arithmeticPostAgg(s, ArithmeticFunction.MINUS),
      expression = ExpressionOps.subtract(Expr(value), Expr(s.getName))
    )
    //def *(s: Dim): ArithmeticPostAgg = arithmeticPostAgg(s, ArithmeticFunction.MULT)
    def *(s: Dim): BaseArithmeticExpression = BaseArithmeticExpression(
      portAgg = arithmeticPostAgg(s, ArithmeticFunction.MULT),
      expression = ExpressionOps.multiply(Expr(value), Expr(s.getName))
    )

    //def /(s: Dim): ArithmeticPostAgg = arithmeticPostAgg(s, ArithmeticFunction.DIV)
    def /(s: Dim): BaseArithmeticExpression = BaseArithmeticExpression(
      portAgg = arithmeticPostAgg(s, ArithmeticFunction.DIV),
      expression = ExpressionOps.divide(Expr(value), Expr(s.getName))
    )

    def %(s: Dim): Expression = ExpressionOps.modulo(Expr(value), Expr(s.getName))

    def quotient(s: Dim): ArithmeticPostAgg =
      arithmeticPostAgg(s, ArithmeticFunction.QUOT)

  }

  implicit class StringToExpression(val sc: StringContext) extends AnyVal {

    /**
      * Create an expression using simple string interpolator.
      *
      * {{{
      *   val value = "something"
      *
      *   expr"dimension == ${value}"
      * }}}
      *
      * @param args The arguments to be inserted into the resulting expression.
      *
      * @see StringContext
      */
    def expr(args: Any*): Expression = Expr(sc.s(args: _*))
  }

  implicit def dimToExpression(d: Dim): Expression = Expr(d.getName)

  implicit def baseExpressionToExpression(b: BaseExpression): Expression = b.asExpression
  implicit def baseExpressionToFilteringExpression(
      b: BaseExpression
  ): FilteringExpression = b.asFilteringExpression

  implicit def baseArithmeticExpressionToArithmeticPostAgg(
      b: BaseArithmeticExpression
  ): ArithmeticPostAgg = b.asArithmeticPostAgg
  implicit def baseArithmeticExpressionToExpression(b: BaseArithmeticExpression): Expression =
    b.asExpression

  sealed trait JoinPart
  case class LeftPart() extends JoinPart {
    def apply(dimName: String): LeftExpresssion = new LeftExpresssion(dimName)
  }
  case class RightPart(prefix: String) extends JoinPart {
    def apply(dimName: String): RightExpresssion = new RightExpresssion(prefix + dimName)
  }

  implicit class DatasourceOps(val left: Datasource) extends AnyVal {

    def joinNew(right: RightHandDatasource,
                prefix: String,
                condition: (LeftPart, RightPart) => Expression,
                joinType: JoinType = JoinType.Inner): Join = {
      val leftPart  = LeftPart()
      val rightPart = RightPart(prefix)

      Join(left = left,
           right = right,
           rightPrefix = prefix,
           condition = condition(leftPart, rightPart).build(),
           joinType = joinType)
    }

//    def join(right: RightHandDatasource,
//             prefix: String,
//             condition: Expression,
//             joinType: JoinType = JoinType.Inner): Join =
//      Join(left = left,
//           right = right,
//           rightPrefix = prefix,
//           condition = condition.build(),
//           joinType = joinType)
  }

}
// scalastyle:on
