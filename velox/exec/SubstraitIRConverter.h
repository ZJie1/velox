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

#include <string>
#include <typeinfo>

#include "velox/substrait_converter/proto/substrait/algebra.pb.h"
#include "velox/substrait_converter/proto/substrait/plan.pb.h"
#include "velox/substrait_converter/proto/substrait/type.pb.h"

#include "connectors/hive/HiveConnector.h"
#include "connectors/hive/HivePartitionFunction.h"
#include "core/PlanNode.h"
#include "exec/HashPartitionFunction.h"
#include "exec/RoundRobinPartitionFunction.h"
#include "expression/Expr.h"
#include "parse/Expressions.h"
#include "parse/ExpressionsParser.h"
#include "type/Type.h"

using namespace facebook::velox::core;

namespace facebook::velox {

class SubstraitVeloxConvertor {
 public:
  void toSubstraitIR(
      std::shared_ptr<const PlanNode> vPlan,
      substrait::Plan& sPlan);

  std::shared_ptr<const PlanNode> fromSubstraitIR(
      const substrait::Plan& sPlan);

 private:
  std::shared_ptr<const PlanNode> fromSubstraitIR(
      const substrait::Plan& sPlan,
      int depth);

  std::shared_ptr<const PlanNode> fromSubstraitIR(
      const substrait::Rel& sRel,
      int depth);

  std::shared_ptr<const ITypedExpr> transformSLiteralExpr(
      const substrait::Expression_Literal& sLiteralExpr);

  variant transformSLiteralType(
      const substrait::Expression_Literal& sLiteralExpr);

  variant processSubstraitLiteralNullType(
      const substrait::Expression_Literal& sLiteralExpr,
      substrait::Type nullType);

  std::shared_ptr<const ITypedExpr> transformSExpr(
      const substrait::Expression& sExpr,
      substrait::NamedStruct* sGlobalMapping);

  std::shared_ptr<FilterNode> transformSFilter(
      const substrait::Rel& sRel,
      int depth);

  std::shared_ptr<PartitionedOutputNode> transformSDistribute(
      const substrait::Plan& sPlan,
      int depth);

  velox::RowTypePtr sNamedStructToVRowTypePtr(
      substrait::NamedStruct sNamedStruct);

  std::shared_ptr<const ITypedExpr> parseExpr(
      const std::string& text,
      std::shared_ptr<const velox::RowType> vRowType);

  std::shared_ptr<PlanNode> transformSRead(
      const substrait::Rel& sRel,
      int depth);

  std::shared_ptr<ProjectNode> transformSProject(
      const substrait::Rel& sRel,
      int depth);

  std::shared_ptr<AggregationNode> transformSAggregate(
      const substrait::Rel& sRel,
      int depth);

  std::shared_ptr<OrderByNode> transformSSort(
      const substrait::Rel& sRel,
      int depth);

  void initFunctionMap();
  std::string FindFunction(uint64_t id);

  velox::TypePtr substraitTypeToVelox(const substrait::Type& sType);

  void toSubstraitIR(
      std::shared_ptr<const PlanNode> vPlanNode,
      substrait::Rel* sRel);

  substrait::NamedStruct* vRowTypePtrToSNamedStruct(
      velox::RowTypePtr vRow,
      substrait::NamedStruct* sNamedStruct);

  substrait::Expression_Literal* processVeloxValueByType(
      substrait::Expression_Literal_Struct* sLitValue,
      substrait::Expression_Literal* sField,
      VectorPtr children);

  substrait::Expression_Literal* processVeloxNullValueByCount(
      std::shared_ptr<const Type> childType,
      std::optional<vector_size_t> nullCount,
      substrait::Expression_Literal_Struct* sLitValue,
      substrait::Expression_Literal* sField);

  substrait::Expression_Literal* processVeloxNullValue(
      substrait::Expression_Literal* sField,
      std::shared_ptr<const Type> childType);

  void transformVFilter(
      std::shared_ptr<const FilterNode> vFilter,
      substrait::FilterRel* sFilterRel,
      substrait::NamedStruct* sGlobalMapping);

  void transformVValuesNode(
      std::shared_ptr<const ValuesNode> vValuesNode,
      substrait::ReadRel* sReadRel);

  void transformVProjNode(
      std::shared_ptr<const ProjectNode> vProjNode,
      substrait::ProjectRel* sProjRel,
      substrait::NamedStruct* sGlobalMapping);

  void transformVPartitionedOutputNode(
      std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode,
      substrait::DistributeRel* sDistRel);

  void transformVExpr(
      substrait::Expression* sExpr,
      const std::shared_ptr<const ITypedExpr>& vExpr,
      substrait::NamedStruct* sGlobalMapping);

  void transformVConstantExpr(
      const velox::variant& vConstExpr,
      substrait::Expression_Literal* sLiteralExpr);

  void transformVPartitionFunc(
      substrait::DistributeRel* sDistRel,
      std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode);

  uint64_t registerSFunction(std::string name);

  void transformVAggregateNode(
      std::shared_ptr<const AggregationNode> vAggNode,
      substrait::AggregateRel* sAggRel,
      substrait::NamedStruct* sGlobalMapping);

  void transformVOrderBy(
      std::shared_ptr<const OrderByNode> vOrderby,
      substrait::SortRel* sSort);

  substrait::Type veloxTypeToSubstrait(
      const velox::TypePtr& vType,
      substrait::Type* sType);

  std::unordered_map<uint64_t, std::string> functions_map;
  std::unordered_map<std::string, uint64_t> function_map;
  uint64_t last_function_id = 0;
  substrait::Plan plan;

  // Substrait is ordinal based field reference implementation. sGlobalMapping
  // is used to tracked the mapping from ID to field reference.
  substrait::NamedStruct* sGlobalMapping =
      new substrait::NamedStruct();
  std::unique_ptr<velox::memory::ScopedMemoryPool> scopedPool =
      velox::memory::getDefaultScopedMemoryPool();
  velox::memory::MemoryPool* pool_;
};
} // namespace facebook::velox
