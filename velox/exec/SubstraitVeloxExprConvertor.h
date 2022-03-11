/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#pragma once

#include "substrait/algebra.pb.h"

#include "core/PlanNode.h"

#include "SubstraitVeloxFuncConvertor.h"
#include "SubstraitVeloxTypeConvertor.h"

using namespace facebook::velox::core;

namespace facebook::velox::substraitconvertor {

class VeloxToSubstraitExprConvertor {
 public:
  void transformVExpr(
      substrait::Expression* sExpr,
      const std::shared_ptr<const ITypedExpr>& vExpr,
      RowTypePtr vPreNodeOutPut);

  void transformVConstantExpr(
      const velox::variant& vConstExpr,
      substrait::Expression_Literal* sLiteralExpr);

 private:
  VeloxToSubstraitTypeConvertor v2STypeConvertor_;
  VeloxToSubstraitFuncConvertor v2SFuncConvertor_;
};

class SubstraitToVeloxExprConvertor {
 public:
  std::shared_ptr<const ITypedExpr> transformSExpr(
      const substrait::Expression& sExpr,
      RowTypePtr vPreNodeOutPut);

  std::shared_ptr<const ITypedExpr> transformSLiteralExpr(
      const substrait::Expression_Literal& sLiteralExpr);

 private:
  SubstraitToVeloxTypeConvertor s2VTypeConvertor_;
  SubstraitToVeloxFuncConvertor s2VFuncConvertor_;
};

} // namespace facebook::velox::substraitconvertor
