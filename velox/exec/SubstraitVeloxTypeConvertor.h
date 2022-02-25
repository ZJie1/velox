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
#include "velox/substrait_converter/proto/substrait/algebra.pb.h"
#include "velox/substrait_converter/proto/substrait/type.pb.h"

#include "core/PlanNode.h"

using namespace facebook::velox::core;

namespace facebook::velox {

class VeloxToSubstraitTypeConvertor {
 public:
  substrait::NamedStruct* vRowTypePtrToSNamedStruct(
      velox::RowTypePtr vRow,
      substrait::NamedStruct* sNamedStruct);

  substrait::Expression_Literal* processVeloxValueByType(
      substrait::Expression_Literal_Struct* sLitValue,
      substrait::Expression_Literal* sField,
      VectorPtr children);

  substrait::Type veloxTypeToSubstrait(
      const velox::TypePtr& vType,
      substrait::Type* sType);

 private:
  substrait::Expression_Literal* processVeloxNullValueByCount(
      std::shared_ptr<const Type> childType,
      std::optional<vector_size_t> nullCount,
      substrait::Expression_Literal_Struct* sLitValue,
      substrait::Expression_Literal* sField);

  substrait::Expression_Literal* processVeloxNullValue(
      substrait::Expression_Literal* sField,
      std::shared_ptr<const Type> childType);
};

class SubstraitToVeloxTypeConvertor {
 public:
  variant transformSLiteralType(
      const substrait::Expression_Literal& sLiteralExpr);

  variant processSubstraitLiteralNullType(
      const substrait::Expression_Literal& sLiteralExpr,
      substrait::Type nullType);

  velox::RowTypePtr sNamedStructToVRowTypePtr(
      substrait::NamedStruct sNamedStruct);

  velox::TypePtr substraitTypeToVelox(const substrait::Type& sType);
};

} // namespace facebook::velox
