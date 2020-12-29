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

package ing.wbaa.druid.dql.expressions

trait BaseExpression { self =>

  def asFilteringExpression: FilteringExpression
  def asExpression: Expression

  def and(other: FilteringExpression): FilteringExpression =
    new And(Seq(self.asFilteringExpression, other))
  def and(other: Expression): Expression = ExpressionOps.and(self.asExpression, other)
  def and(other: BaseExpression): BaseExpression = BaseExpression(
    filtering = and(other.asFilteringExpression),
    expression = and(other.asExpression)
  )

  def or(other: FilteringExpression): FilteringExpression =
    new Or(Seq(self.asFilteringExpression, other))
  def or(other: Expression): Expression = ExpressionOps.or(self.asExpression, other)
  def or(other: BaseExpression): BaseExpression = BaseExpression(
    filtering = or(other.asFilteringExpression),
    expression = or(other.asExpression)
  )

}

object BaseExpression {

  def apply(filtering: FilteringExpression, expression: Expression): BaseExpression =
    new BaseExpression {
      override def asFilteringExpression: FilteringExpression = filtering
      override def asExpression: Expression                   = expression
    }
}

trait BaseArithmeticExpression { self =>
  def asArithmeticPostAgg: ArithmeticPostAgg
  def asExpression: Expression
}

object BaseArithmeticExpression {

  def apply(portAgg: ArithmeticPostAgg, expression: Expression): BaseArithmeticExpression =
    new BaseArithmeticExpression {
      override def asArithmeticPostAgg: ArithmeticPostAgg = portAgg
      override def asExpression: Expression               = expression
    }
}
