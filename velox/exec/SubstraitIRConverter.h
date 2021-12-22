/*
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

#include "connectors/hive/HiveConnector.h"
#include "connectors/hive/HivePartitionFunction.h"
#include "parse/Expressions.h"
#include "parse/ExpressionsParser.h"
#include "exec/HashPartitionFunction.h"
#include "exec/RoundRobinPartitionFunction.h"
#include "expression/Expr.h"
#include "type/Type.h"

#include "expression.pb.h"
#include "plan.pb.h"
#include "relations.pb.h"
#include "core/PlanNode.h"

using namespace facebook::velox::core;

namespace facebook::velox {

class SubstraitVeloxConvertor {
public:
 void toSubstraitIR(
     std::shared_ptr<const PlanNode> vPlan,
     io::substrait::Plan& sPlan);

 std::shared_ptr<const PlanNode> fromSubstraitIR(
     const io::substrait::Plan& sPlan);

private:
 std::shared_ptr<const PlanNode> fromSubstraitIR(
     const io::substrait::Plan& sPlan,
     int depth);

 std::shared_ptr<const PlanNode> fromSubstraitIR(
     const io::substrait::Rel& sRel,
     int depth);

 std::shared_ptr<const ITypedExpr> transformSLiteralExpr(
     const io::substrait::Expression_Literal& sLiteralExpr);

 std::shared_ptr<const ITypedExpr> transformSExpr(
     const io::substrait::Expression& sExpr,
     io::substrait::Type_NamedStruct*  sGlobalMapping);

 std::shared_ptr<FilterNode> transformSFilter(
     const io::substrait::Rel& sRel,
     int depth);

 std::shared_ptr<PartitionedOutputNode> transformSDistribute(
     const io::substrait::Plan& sPlan,
     int depth);

 velox::RowTypePtr sNamedStructToVRowTypePtr(
     io::substrait::Type_NamedStruct sNamedStruct);

 std::shared_ptr<const ITypedExpr> parseExpr(
     const std::string& text,
     std::shared_ptr<const velox::RowType> vRowType);

 std::shared_ptr<PlanNode> transformSRead(
     const io::substrait::Rel& sRel,
     int depth);

 std::shared_ptr<ProjectNode> transformSProject(
     const io::substrait::Rel& sRel,
     int depth);

 std::shared_ptr<AggregationNode> transformSAggregate(
     const io::substrait::Rel& sRel,
     int depth);

 std::shared_ptr<OrderByNode> transformSSort(
     const io::substrait::Rel& sRel,
     int depth);

 void initFunctionMap(io::substrait::Plan& sPlan);
 std::string FindFunction(uint64_t id);

 velox::TypePtr substraitTypeToVelox(const io::substrait::Type& sType);

 void toSubstraitIR(
     std::shared_ptr<const PlanNode> vPlanNode,
     io::substrait::Rel* sRel);

 io::substrait::Type_NamedStruct* vRowTypePtrToSNamedStruct(
     velox::RowTypePtr vRow,
     io::substrait::Type_NamedStruct* sNamedStruct);

 void transformVFilter(
     std::shared_ptr<const FilterNode> vFilter,
     io::substrait::FilterRel* sFilterRel,
     io::substrait::Type_NamedStruct* sGlobalMapping);

 void transformVValuesNode(
     std::shared_ptr<const ValuesNode> vValuesNode,
     io::substrait::ReadRel* sReadRel);

 void transformVProjNode(
     std::shared_ptr<const ProjectNode> vProjNode,
     io::substrait::ProjectRel* sProjRel,
     io::substrait::Type_NamedStruct* sGlobalMapping);

 void transformVPartitionedOutputNode(
     std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode,
     io::substrait::DistributeRel* sDistRel);

 void transformVExpr(
     io::substrait::Expression* sExpr,
     const std::shared_ptr<const ITypedExpr>& vExpr,
     io::substrait::Type_NamedStruct* sGlobalMapping);

 void transformVConstantExpr(
     const velox::variant& vConstExpr,
     io::substrait::Expression_Literal* sLiteralExpr);

 void transformVPartitionFunc(
     io::substrait::DistributeRel* sDistRel,
     std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode);

 uint64_t registerSFunction(std::string name);

 void transformVAgg(
     std::shared_ptr<const AggregationNode> vAgg,
     io::substrait::AggregateRel* sAgg);

 void transformVOrderBy(
     std::shared_ptr<const OrderByNode> vOrderby,
     io::substrait::SortRel* sSort);

 io::substrait::Type veloxTypeToSubstrait(
     const velox::TypePtr& vType,
     io::substrait::Type* sType);

 std::unordered_map<uint64_t, std::string> functions_map;
 std::unordered_map<std::string, uint64_t> function_map;
 uint64_t last_function_id = 0;
 io::substrait::Plan plan;

 io::substrait::Type_NamedStruct*  sGlobalMapping = new io::substrait::Type_NamedStruct();
 std::unique_ptr<velox::memory::ScopedMemoryPool> scopedPool = velox::memory::getDefaultScopedMemoryPool();
 velox::memory::MemoryPool* pool_;
};
} // namespace facebook::velox
