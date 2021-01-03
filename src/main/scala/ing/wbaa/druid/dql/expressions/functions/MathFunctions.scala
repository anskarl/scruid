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

import ing.wbaa.druid.dql.expressions.{ CastType, Expression, ExpressionLiteral, LeftExpression }

// scalastyle:off number.of.methods
trait MathFunctions {
  import GeneralFunctions.cast

  def abs[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("abs", x)
  def abs[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = abs(cast(x, castType))

  def acos[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("acos", x)
  def acos[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = acos(cast(x, castType))

  def asin[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("asin", x)
  def asin[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = asin(cast(x, castType))

  def atan[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("atan", x)
  def atan[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = atan(cast(x, castType))

  def atan2[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, y: T2): LeftExpression =
    Expression.fun("atan2", x, y)

  def atan2[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                          y: T2,
                                                          xCastType: CastType,
                                                          yCastType: CastType): LeftExpression =
    atan2(cast(x, xCastType), cast(y, yCastType))

  def cbrt[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("cbrt", x)
  def cbrt[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = cbrt(cast(x, castType))

  def ceil[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("ceil", x)
  def ceil[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = ceil(cast(x, castType))

  def copysign[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, y: T2): LeftExpression =
    Expression.fun("copysign", x, y)

  def copysign[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                             y: T2,
                                                             xCastType: CastType,
                                                             yCastType: CastType): LeftExpression =
    copysign(cast(x, xCastType), cast(y, yCastType))

  def cos[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("cos", x)
  def cos[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = cos(cast(x, castType))

  def cosh[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("cosh", x)
  def cosh[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = cosh(cast(x, castType))

  def cot[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("cot", x)
  def cot[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = cot(cast(x, castType))

  def div[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, y: T2): LeftExpression =
    Expression.fun("div", x, y)
  def div[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                        y: T2,
                                                        castType: CastType): LeftExpression =
    div(cast(x, castType), cast(y, castType))

  def exp[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("exp", x)
  def exp[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = exp(cast(x, castType))

  def expm1[T: ExpressionLiteral](x: T): LeftExpression = Expression.fun("expm1", x)
  def expm1[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    expm1(cast(x, castType))

  def floor[T: ExpressionLiteral](x: T): LeftExpression = Expression.fun("floor", x)
  def floor[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    floor(cast(x, castType))

  def getExponent[T: ExpressionLiteral](x: T): LeftExpression =
    Expression.fun("getExponent", x)
  def getExponent[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    getExponent(cast(x, castType))

  def hypot[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, y: T2): LeftExpression =
    Expression.fun("hypot", x, y)

  def hypot[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                          y: T2,
                                                          xCastType: CastType,
                                                          yCastType: CastType): LeftExpression =
    hypot(cast(x, xCastType), cast(y, yCastType))

  def log[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("log", x)
  def log[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = log(cast(x, castType))

  def log10[T: ExpressionLiteral](x: T): LeftExpression = Expression.fun("log10", x)
  def log10[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    log10(cast(x, castType))

  def log1p[T: ExpressionLiteral](x: T): LeftExpression = Expression.fun("log1p", x)
  def log1p[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    log1p(cast(x, castType))

  def max[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, y: T2): LeftExpression =
    Expression.fun("max", x, y)

  def max[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                        y: T2,
                                                        xCastType: CastType,
                                                        yCastType: CastType): LeftExpression =
    max(cast(x, xCastType), cast(y, yCastType))

  def min[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, y: T2): LeftExpression =
    Expression.fun("min", x, y)

  def min[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                        y: T2,
                                                        xCastType: CastType,
                                                        yCastType: CastType): LeftExpression =
    min(cast(x, xCastType), cast(y, yCastType))

  def nextAfter[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, y: T2): LeftExpression =
    Expression.fun("nextAfter", x, y)

  def nextAfter[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                              y: T2,
                                                              xCastType: CastType,
                                                              yCastType: CastType): LeftExpression =
    nextAfter(cast(x, xCastType), cast(y, yCastType))

  def nextUp[T: ExpressionLiteral](x: T): LeftExpression = Expression.fun("nextUp", x)
  def nextUp[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    nextUp(cast(x, castType))

  def pi: LeftExpression = Expression.fun("pi")

  def pow[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, y: T2): LeftExpression =
    Expression.fun("pow", x, y)
  def pow[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                        y: T2,
                                                        xCastType: CastType,
                                                        yCastType: CastType): LeftExpression =
    pow(cast(x, xCastType), cast(y, yCastType))

  def remainder[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, y: T2): LeftExpression =
    Expression.fun("remainder", x, y)
  def remainder[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                              y: T2,
                                                              xCastType: CastType,
                                                              yCastType: CastType): LeftExpression =
    remainder(cast(x, xCastType), cast(y, yCastType))

  def rint[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("rint", x)
  def rint[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = rint(cast(x, castType))

  def round[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1, scale: T2): LeftExpression =
    Expression.fun("round", x, cast(scale, CastType.Long))

  def round[T: ExpressionLiteral](x: T): LeftExpression = round(x, scale = 0)

  def round[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    round(x, scale = 0, xCastType = castType)

  def round[T1: ExpressionLiteral, T2: ExpressionLiteral](x: T1,
                                                          scale: T2,
                                                          xCastType: CastType): LeftExpression =
    round(cast(x, xCastType), scale)

  def scalb[T1: ExpressionLiteral, T2: ExpressionLiteral](d: T1, sf: T2): LeftExpression =
    Expression.fun("scalb", d, sf)

  def scalb[T1: ExpressionLiteral, T2: ExpressionLiteral](d: T1,
                                                          sf: T2,
                                                          xCastType: CastType,
                                                          yCastType: CastType): LeftExpression =
    scalb(cast(d, xCastType), cast(sf, yCastType))

  def signum[T: ExpressionLiteral](x: T): LeftExpression = Expression.fun("signum", x)
  def signum[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    signum(cast(x, castType))

  def sin[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("sin", x)
  def sin[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = sin(cast(x, castType))

  def sinh[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("sinh", x)
  def sinh[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = sinh(cast(x, castType))

  def sqrt[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("sqrt", x)
  def sqrt[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = sqrt(cast(x, castType))

  def tan[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("tan", x)
  def tan[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = tan(cast(x, castType))

  def tanh[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("tanh", x)
  def tanh[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = tanh(cast(x, castType))

  def todegrees[T: ExpressionLiteral](x: T): LeftExpression = Expression.fun("todegrees", x)
  def todegrees[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    todegrees(cast(x, castType))

  def toradians[T: ExpressionLiteral](x: T): LeftExpression = Expression.fun("toradians", x)
  def toradians[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression =
    toradians(cast(x, castType))

  def ulp[T: ExpressionLiteral](x: T): LeftExpression                     = Expression.fun("ulp", x)
  def ulp[T: ExpressionLiteral](x: T, castType: CastType): LeftExpression = ulp(cast(x, castType))

}
// scalastyle:on number.of.methods

object MathFunctions extends MathFunctions
