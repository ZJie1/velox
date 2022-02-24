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

using namespace facebook::velox::core;

namespace facebook::velox {

class VeloxToSubstraitTypeConvertor {
 public:
  io::substrait::Type_NamedStruct* vRowTypePtrToSNamedStruct(
      velox::RowTypePtr vRow,
      io::substrait::Type_NamedStruct* sNamedStruct);

  io::substrait::Expression_Literal* processVeloxValueByType(
      io::substrait::Expression_Literal_Struct* sLitValue,
      io::substrait::Expression_Literal* sField,
      VectorPtr children);

  io::substrait::Type veloxTypeToSubstrait(
      const velox::TypePtr& vType,
      io::substrait::Type* sType);

 private:
  io::substrait::Expression_Literal* processVeloxNullValueByCount(
      std::shared_ptr<const Type> childType,
      std::optional<vector_size_t> nullCount,
      io::substrait::Expression_Literal_Struct* sLitValue,
      io::substrait::Expression_Literal* sField);

  io::substrait::Expression_Literal* processVeloxNullValue(
      io::substrait::Expression_Literal* sField,
      std::shared_ptr<const Type> childType);
};

class SubstraitToVeloxTypeConvertor {
 public:
  variant transformSLiteralType(
      const io::substrait::Expression_Literal& sLiteralExpr);

  variant processSubstraitLiteralNullType(
      const io::substrait::Expression_Literal& sLiteralExpr,
      io::substrait::Type nullType);

  velox::RowTypePtr sNamedStructToVRowTypePtr(
      io::substrait::Type_NamedStruct sNamedStruct);

  velox::TypePtr substraitTypeToVelox(const io::substrait::Type& sType);
};

} // namespace facebook::velox
