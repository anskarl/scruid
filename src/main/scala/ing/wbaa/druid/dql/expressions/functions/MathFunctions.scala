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

import ing.wbaa.druid.dql.expressions.{ CastType, Expression, LeftExpression }

// scalastyle:off number.of.methods
trait MathFunctions {
  import GeneralFunctions.cast

  def abs(x: LeftExpression): LeftExpression                     = new LeftExpression(s"abs(${x.build()})")
  def abs(x: LeftExpression, castType: CastType): LeftExpression = abs(cast(x, castType))

  def acos(x: LeftExpression): LeftExpression                     = Expression.function("acos", x.build())
  def acos(x: LeftExpression, castType: CastType): LeftExpression = acos(cast(x, castType))

  def asin(x: LeftExpression): LeftExpression                     = Expression.function("asin", x.build())
  def asin(x: LeftExpression, castType: CastType): LeftExpression = asin(cast(x, castType))

  def atan(x: LeftExpression): LeftExpression                     = Expression.function("atan", x.build())
  def atan(x: LeftExpression, castType: CastType): LeftExpression = atan(cast(x, castType))

  def atan2(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expression.function("atan2", x.build(), y.build())

  def atan2(x: LeftExpression,
            y: LeftExpression,
            xCastType: CastType,
            yCastType: CastType): LeftExpression =
    atan2(cast(x, xCastType), cast(y, yCastType))

  def cbrt(x: LeftExpression): LeftExpression                     = Expression.function("cbrt", x.build())
  def cbrt(x: LeftExpression, castType: CastType): LeftExpression = cbrt(cast(x, castType))

  def ceil(x: LeftExpression): LeftExpression                     = Expression.function("ceil", x.build())
  def ceil(x: LeftExpression, castType: CastType): LeftExpression = ceil(cast(x, castType))

  def copysign(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expression.function("copysign", x.build(), y.build())

  def copysign(x: LeftExpression,
               y: LeftExpression,
               xCastType: CastType,
               yCastType: CastType): LeftExpression =
    copysign(cast(x, xCastType), cast(y, yCastType))

  def cos(x: LeftExpression): LeftExpression                     = Expression.function("cos", x.build())
  def cos(x: LeftExpression, castType: CastType): LeftExpression = cos(cast(x, castType))

  def cosh(x: LeftExpression): LeftExpression                     = Expression.function("cosh", x.build())
  def cosh(x: LeftExpression, castType: CastType): LeftExpression = cosh(cast(x, castType))

  def cot(x: LeftExpression): LeftExpression                     = Expression.function("cot", x.build())
  def cot(x: LeftExpression, castType: CastType): LeftExpression = cot(cast(x, castType))

  def div(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expression.function("div", x.build(), y.build())
  def div(x: LeftExpression, y: LeftExpression, castType: CastType): LeftExpression =
    div(cast(x, castType), cast(y, castType))

  def exp(x: LeftExpression): LeftExpression                     = Expression.function("exp", x.build())
  def exp(x: LeftExpression, castType: CastType): LeftExpression = exp(cast(x, castType))

  def expm1(x: LeftExpression): LeftExpression                     = Expression.function("expm1", x.build())
  def expm1(x: LeftExpression, castType: CastType): LeftExpression = expm1(cast(x, castType))

  def floor(x: LeftExpression): LeftExpression                     = Expression.function("floor", x.build())
  def floor(x: LeftExpression, castType: CastType): LeftExpression = floor(cast(x, castType))

  def getExponent(x: LeftExpression): LeftExpression =
    Expression.function("getExponent", x.build())
  def getExponent(x: LeftExpression, castType: CastType): LeftExpression =
    getExponent(cast(x, castType))

  def hypot(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expression.function("hypot", x.build(), y.build())

  def hypot(x: LeftExpression,
            y: LeftExpression,
            xCastType: CastType,
            yCastType: CastType): LeftExpression =
    hypot(cast(x, xCastType), cast(y, yCastType))

  def log(x: LeftExpression): LeftExpression                     = Expression.function("log", x.build())
  def log(x: LeftExpression, castType: CastType): LeftExpression = log(cast(x, castType))

  def log10(x: LeftExpression): LeftExpression                     = Expression.function("log10", x.build())
  def log10(x: LeftExpression, castType: CastType): LeftExpression = log10(cast(x, castType))

  def log1p(x: LeftExpression): LeftExpression                     = Expression.function("log1p", x.build())
  def log1p(x: LeftExpression, castType: CastType): LeftExpression = log1p(cast(x, castType))

  def max(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expression.function("max", x.build(), y.build())

  def max(x: LeftExpression,
          y: LeftExpression,
          xCastType: CastType,
          yCastType: CastType): LeftExpression =
    max(cast(x, xCastType), cast(y, yCastType))

  def min(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expression.function("min", x.build(), y.build())

  def min(x: LeftExpression,
          y: LeftExpression,
          xCastType: CastType,
          yCastType: CastType): LeftExpression =
    min(cast(x, xCastType), cast(y, yCastType))

  def nextAfter(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expression.function("nextAfter", x.build(), y.build())

  def nextAfter(x: LeftExpression,
                y: LeftExpression,
                xCastType: CastType,
                yCastType: CastType): LeftExpression =
    nextAfter(cast(x, xCastType), cast(y, yCastType))

  def nextUp(x: LeftExpression): LeftExpression                     = Expression.function("nextUp", x.build())
  def nextUp(x: LeftExpression, castType: CastType): LeftExpression = nextUp(cast(x, castType))

  def pi: LeftExpression = Expression.function("pi")

  def pow(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expression.function("pow", x.build(), y.build())
  def pow(x: LeftExpression,
          y: LeftExpression,
          xCastType: CastType,
          yCastType: CastType): LeftExpression =
    pow(cast(x, xCastType), cast(y, yCastType))

  def remainder(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expression.function("remainder", x.build(), y.build())
  def remainder(x: LeftExpression,
                y: LeftExpression,
                xCastType: CastType,
                yCastType: CastType): LeftExpression =
    remainder(cast(x, xCastType), cast(y, yCastType))

  def rint(x: LeftExpression): LeftExpression                     = Expression.function("rint", x.build())
  def rint(x: LeftExpression, castType: CastType): LeftExpression = rint(cast(x, castType))

  def round(x: LeftExpression, scale: LeftExpression): LeftExpression =
    Expression.function("round", x.build(), scale.cast(CastType.Long).build())

  def round(x: LeftExpression, scale: Int): LeftExpression =
    Expression.function("round", x.build(), scale.toString)

  def round(x: LeftExpression): LeftExpression = round(x, scale = 0)

  def round(x: LeftExpression, castType: CastType, scale: Int): LeftExpression =
    round(cast(x, castType), scale)
  def round(x: LeftExpression, castType: CastType): LeftExpression = round(x, castType, scale = 0)

  def round(x: LeftExpression, scale: LeftExpression, castType: CastType): LeftExpression =
    round(x.cast(castType), scale)

  def scalb(d: LeftExpression, sf: Int): LeftExpression =
    Expression.function("scalb", d.build(), sf.toString)

  def scalb(d: LeftExpression, sf: LeftExpression): LeftExpression =
    Expression.function("scalb", d.build(), sf.build())

  def scalb(d: LeftExpression,
            sf: LeftExpression,
            xCastType: CastType,
            yCastType: CastType): LeftExpression =
    scalb(cast(d, xCastType), cast(sf, yCastType))

  def signum(x: LeftExpression): LeftExpression                     = Expression.function("signum", x.build())
  def signum(x: LeftExpression, castType: CastType): LeftExpression = signum(cast(x, castType))

  def sin(x: LeftExpression): LeftExpression                     = Expression.function("sin", x.build())
  def sin(x: LeftExpression, castType: CastType): LeftExpression = sin(cast(x, castType))

  def sinh(x: LeftExpression): LeftExpression                     = Expression.function("sinh", x.build())
  def sinh(x: LeftExpression, castType: CastType): LeftExpression = sinh(cast(x, castType))

  def sqrt(x: LeftExpression): LeftExpression                     = Expression.function("sqrt", x.build())
  def sqrt(x: LeftExpression, castType: CastType): LeftExpression = sqrt(cast(x, castType))

  def tan(x: LeftExpression): LeftExpression                     = Expression.function("tan", x.build())
  def tan(x: LeftExpression, castType: CastType): LeftExpression = tan(cast(x, castType))

  def tanh(x: LeftExpression): LeftExpression                     = Expression.function("tanh", x.build())
  def tanh(x: LeftExpression, castType: CastType): LeftExpression = tanh(cast(x, castType))

  def todegrees(x: LeftExpression): LeftExpression = Expression.function("todegrees", x.build())
  def todegrees(x: LeftExpression, castType: CastType): LeftExpression =
    todegrees(cast(x, castType))

  def toradians(x: LeftExpression): LeftExpression = Expression.function("toradians", x.build())
  def toradians(x: LeftExpression, castType: CastType): LeftExpression =
    toradians(cast(x, castType))

  def ulp(x: LeftExpression): LeftExpression                     = Expression.function("ulp", x.build())
  def ulp(x: LeftExpression, castType: CastType): LeftExpression = ulp(cast(x, castType))

}
// scalastyle:on number.of.methods

object MathFunctions extends MathFunctions
