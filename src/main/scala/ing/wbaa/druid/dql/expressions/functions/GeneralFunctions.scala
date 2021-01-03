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

package ing.wbaa.druid.dql.expressions.functions

import ing.wbaa.druid.dql.expressions.{
  BaseExpression,
  CastType,
  Expression,
  ExpressionLiteral,
  LeftExpression
}

object CaseSearched {

  def builder: CaseBuilder = new CaseBuilder()

  sealed trait CaseSearchedBuilder {
    private[expressions] def build(): Expression
  }

  class BuilderOtherwise private[expressions] (caseStatements: Seq[(Expression, Expression)],
                                               elseResult: Expression)
      extends CaseSearchedBuilder {
    override private[expressions] def build(): Expression = BaseExpression {
      caseStatements
        .map { case (condition, result) => s"${condition.build()}, ${result.build()}" }
        .mkString(s"case_searched(", ",", s"${elseResult.build()})")
    }
  }

  class CaseBuilder private[expressions] () extends CaseSearchedBuilder { self =>
    // scalastyle:off var.field
    private var caseStatements = Seq.empty[(Expression, Expression)]
    // scalastyle:on var.field

    def when(condition: Expression, result: Expression): self.type = {
      caseStatements :+= condition -> result
      self
    }

    def otherwise(elseResult: Expression): CaseSearchedBuilder =
      new BuilderOtherwise(caseStatements, elseResult)

    override private[expressions] def build(): Expression = BaseExpression {
      caseStatements
        .map { case (condition, result) => s"${condition.build()}, ${result.build()}" }
        .mkString(s"case_searched(", ",", ")")
    }
  }
}

object CaseSimple {

  def builder(expression: Expression): CaseBuilder = new CaseBuilder(expression)

  sealed trait CaseSimpleBuilder {
    private[expressions] def build(): Expression
  }

  class BuilderOtherwise private[expressions] (expression: Expression,
                                               valueResults: Seq[(Expression, Expression)],
                                               elseResult: Expression)
      extends CaseSimpleBuilder {
    override private[expressions] def build(): Expression = BaseExpression {
      valueResults
        .map { case (condition, result) => s"${condition.build()}, ${result.build()}" }
        .mkString(s"case_simple(${expression.build()}", ",", s"${elseResult.build()})")
    }
  }

  class CaseBuilder private[expressions] (expression: Expression) extends CaseSimpleBuilder {
    self =>
    // scalastyle:off var.field
    private var valueResults = Seq.empty[(Expression, Expression)]
    // scalastyle:on var.field

    def when(condition: Expression, result: Expression): self.type = {
      valueResults :+= condition -> result
      self
    }

    def otherwise(elseResult: Expression): CaseSimpleBuilder =
      new BuilderOtherwise(expression, valueResults, elseResult)

    override private[expressions] def build(): Expression = BaseExpression {
      valueResults
        .map { case (condition, result) => s"${condition.build()}, ${result.build()}" }
        .mkString(s"case_simple(${expression.build()}", ",", ")")
    }
  }
}

// scalastyle:off todo.comment
trait GeneralFunctions {

  def cast[T: ExpressionLiteral](expr: T, castType: CastType): LeftExpression =
    Expression.fun("cast", expr, castType.encode())

  def when(predicateExpr: LeftExpression,
           thenExpr: LeftExpression,
           elseExpr: Expression): LeftExpression =
    new LeftExpression(s"if(${predicateExpr.build()}, ${thenExpr.build()}, ${elseExpr.build()})")

  def nvl(expr: LeftExpression, exprWhenNull: LeftExpression): LeftExpression =
    new LeftExpression(s"nvl(${expr.build()}, ${exprWhenNull.build()})")

  /**
    * Builder to create case_searched function expressions:
    *
    * For example to create the following:
    * {{{
    * case_searched(expr1, result1, [[expr2, result2, ...], else-result])
    * }}}
    *
    * {{{
    *   caseSearched{ _
    *     .when(Expr("expr1"), Expr("result1"))
    *     .when(Expr("expr2"), Expr("result2"))
    *     .otherwise(Expr("else-result"))
    *   }
    * }}}
    * @param builder CaseBuilder to create the expression
    * @return the resulting case_searched expression
    */
  def caseSearched(
      builder: CaseSearched.CaseBuilder => CaseSearched.CaseSearchedBuilder
  ): Expression =
    builder(CaseSearched.builder).build()

  /**
    * Builder to create case_simple function expressions:
    *
    * For example to create the following:
    * {{{
    * case_simple(expr, value1, result1, [[value2, result2, ...], else-result])
    * }}}
    *
    * {{{
    *   caseSimple(Expr("expr"), { _
    *     .when(Expr("value1"), Expr("result1"))
    *     .when(Expr("value2"), Expr("result2"))
    *     .otherwise(Expr("else-result"))
    *   })
    * }}}
    *
    * @param builder CaseBuilder to create the expression
    * @return the resulting case_simple expression
    */
  def caseSimple(expression: Expression,
                 builder: CaseSimple.CaseBuilder => CaseSimple.CaseSimpleBuilder): Expression =
    builder(CaseSimple.builder(expression)).build()

  // todo bloom_filter_test

}
// scalastyle:on todo.comment

object GeneralFunctions extends GeneralFunctions
