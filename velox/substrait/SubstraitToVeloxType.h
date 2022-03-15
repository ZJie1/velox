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
#include "velox/substrait/proto/substrait/algebra.pb.h"
#include "velox/substrait/proto/substrait/type.pb.h"

#include "core/PlanNode.h"

using namespace facebook::velox::core;

namespace facebook::velox::substrait {

class SubstraitToVeloxTypeConvertor {
 public:
  variant transformSLiteralType(
      const ::substrait::Expression_Literal& sLiteralExpr);

  variant processSubstraitLiteralNullType(
      const ::substrait::Expression_Literal& sLiteralExpr,
      ::substrait::Type nullType);

  velox::RowTypePtr sNamedStructToVRowTypePtr(
      ::substrait::NamedStruct sNamedStruct);

  velox::TypePtr substraitTypeToVelox(const ::substrait::Type& sType);
};

} // namespace facebook::velox::substrait
