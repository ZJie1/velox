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

#include "expression.pb.h"

#include "core/PlanNode.h"

#include "SubstraitVeloxFuncConvertor.h"
#include "SubstraitVeloxTypeConvertor.h"

using namespace facebook::velox::core;

namespace facebook::velox {

class VeloxToSubstraitExprConvertor {
 public:
  void transformVExpr(
      io::substrait::Expression* sExpr,
      const std::shared_ptr<const ITypedExpr>& vExpr,
      io::substrait::Type_NamedStruct* sGlobalMapping);

  void transformVConstantExpr(
      const velox::variant& vConstExpr,
      io::substrait::Expression_Literal* sLiteralExpr);

 private:
  VeloxToSubstraitTypeConvertor v2STypeConvertor;
  VeloxToSubstraitFuncConvertor v2SFuncConvertor;
};

class SubstraitToVeloxExprConvertor {
 public:
  std::shared_ptr<const ITypedExpr> transformSExpr(
      const io::substrait::Expression& sExpr,
      io::substrait::Type_NamedStruct* sGlobalMapping);

  std::shared_ptr<const ITypedExpr> transformSLiteralExpr(
      const io::substrait::Expression_Literal& sLiteralExpr);

 private:
  SubstraitToVeloxTypeConvertor s2VTypeConvertor;
  SubstraitToVeloxFuncConvertor s2VFuncConvertor;
};

} // namespace facebook::velox

