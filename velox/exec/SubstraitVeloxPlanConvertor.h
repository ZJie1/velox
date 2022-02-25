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

#include <string>
#include <typeinfo>

#include "velox/substrait_converter/proto/substrait/algebra.pb.h"
#include "velox/substrait_converter/proto/substrait/plan.pb.h"

#include "connectors/hive/HiveConnector.h"
#include "connectors/hive/HivePartitionFunction.h"
#include "core/PlanNode.h"
#include "exec/HashPartitionFunction.h"
#include "exec/RoundRobinPartitionFunction.h"
#include "type/Type.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"

#include "SubstraitVeloxExprConvertor.h"

using namespace facebook::velox::core;

namespace facebook::velox {

class VeloxToSubstraitPlanConvertor {
 public:
  void veloxToSubstraitIR(
      std::shared_ptr<const PlanNode> vPlan,
      substrait::Plan& sPlan);

 private:
  void veloxToSubstraitIR(
      std::shared_ptr<const PlanNode> vPlanNode,
      substrait::Rel* sRel);
  void transformVFilter(
      std::shared_ptr<const FilterNode> vFilterNode,
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

  void transformVPartitionFunc(
      substrait::DistributeRel* sDistRel,
      std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode);

  void transformVAggregateNode(
      std::shared_ptr<const AggregationNode> vAggNode,
      substrait::AggregateRel* sAggRel,
      substrait::NamedStruct* sGlobalMapping);

  void transformVOrderBy(
      std::shared_ptr<const OrderByNode> vOrderbyNode,
      substrait::SortRel* sSortRel);

  VeloxToSubstraitExprConvertor v2SExprConvertor;
  VeloxToSubstraitTypeConvertor v2STypeConvertor;
  VeloxToSubstraitFuncConvertor v2SFuncConvertor;
};

class SubstraitToVeloxPlanConvertor {
 public:
  std::shared_ptr<const PlanNode> substraitIRToVelox(
      const substrait::Plan& sPlan);

 private:
  std::shared_ptr<const PlanNode> substraitIRToVelox(
      const substrait::Rel& sRel,
      int depth);

  std::shared_ptr<FilterNode> transformSFilter(
      const substrait::Rel& sRel,
      int depth);

  std::shared_ptr<PartitionedOutputNode> transformSDistribute(
      const substrait::Plan& sPlan,
      int depth);

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

  std::unique_ptr<velox::memory::ScopedMemoryPool> scopedPool =
      velox::memory::getDefaultScopedMemoryPool();
  velox::memory::MemoryPool* pool_;

  SubstraitToVeloxFuncConvertor s2VFuncConvertor;
  SubstraitToVeloxExprConvertor s2VExprConvertor;
  SubstraitToVeloxTypeConvertor s2VTypeConvertor;
};

} // namespace facebook::velox

