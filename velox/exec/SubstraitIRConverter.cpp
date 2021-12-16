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

#include "SubstraitIRConverter.h"

namespace facebook::velox {

// Consume substrait rel
// Public API:
std::shared_ptr<const PlanNode> SubstraitVeloxConvertor::fromSubstraitIR(
    const io::substrait::Plan& sPlan) {
  return fromSubstraitIR(sPlan, 0);
};

// Private APIs:
/**
 *
 * @param plan
 * @param depth means the plan id, assuming node is kept in inserted order. For
 * example, source is located at position 0.
 * @return
 */
std::shared_ptr<const PlanNode> SubstraitVeloxConvertor::fromSubstraitIR(
    const io::substrait::Plan& sPlan,
    int depth) {
  initFunctionMap(const_cast<io::substrait::Plan&>(sPlan));
  const io::substrait::Rel& sRel = sPlan.relations(depth);
  fromSubstraitIR(sRel, depth);
}

std::shared_ptr<const PlanNode> SubstraitVeloxConvertor::fromSubstraitIR(
    const io::substrait::Rel& sRel,
    int depth) {
  switch (sRel.RelType_case()) {
    case io::substrait::Rel::RelTypeCase::kFilter:
      return transformSFilter(sRel, depth);
    case io::substrait::Rel::RelTypeCase::kSort:
      return transformSSort(sRel, depth);
    case io::substrait::Rel::RelTypeCase::kFetch:
    case io::substrait::Rel::RelTypeCase::kRead: {
      return transformSRead(sRel, depth);
    }
    case io::substrait::Rel::RelTypeCase::kAggregate: {
      return transformSAggregate(sRel, depth);
    }
    case io::substrait::Rel::RelTypeCase::kProject: {
      return transformSProject(sRel, depth);
    }
    case io::substrait::Rel::RelTypeCase::kJoin:
    case io::substrait::Rel::RelTypeCase::kSet:
    case io::substrait::Rel::RelTypeCase::kDistribute:
    default:
      throw std::runtime_error(
          "Unsupported relation type " + std::to_string(sRel.RelType_case()));
  }
}

void SubstraitVeloxConvertor::initFunctionMap(io::substrait::Plan& sPlan) {
  for (auto& sMap : sPlan.mappings()) {
    if (!sMap.has_function_mapping()) {
      continue;
    }
    auto& sFunMap = sMap.function_mapping();
    functions_map[sFunMap.function_id().id()] = sFunMap.name();
  }
}

std::string SubstraitVeloxConvertor::FindFunction(uint64_t id) {
  if (functions_map.find(id) == functions_map.end()) {
    throw std::runtime_error(
        "Could not find aggregate function " + std::to_string(id));
  }
  return functions_map[id];
}

velox::TypePtr SubstraitVeloxConvertor::substraitTypeToVelox(
    const io::substrait::Type& sType) {
  switch (sType.kind_case()) {
    case io::substrait::Type::kFixedBinary:
    case io::substrait::Type::kBinary: {
      return velox::TypePtr(velox::VARBINARY());
    }
    case io::substrait::Type::kString:
    case io::substrait::Type::kFixedChar:
    case io::substrait::Type::kVarchar: {
      return velox::TypePtr(velox::VARCHAR());
    }
    case io::substrait::Type::kI8: {
      return velox::TypePtr(velox::TINYINT());
    }
    case io::substrait::Type::kI16: {
      return velox::TypePtr(velox::SMALLINT());
    }
    case io::substrait::Type::kI32: {
      return velox::TypePtr(velox::INTEGER());
    }
    case io::substrait::Type::kI64: {
      return velox::TypePtr(velox::BIGINT());
    }
    case io::substrait::Type::kBool: {
      return velox::TypePtr(velox::BOOLEAN());
    }
    case io::substrait::Type::kFp32: {
      return velox::TypePtr(velox::REAL());
    }
    case io::substrait::Type::kDecimal:
    case io::substrait::Type::kFp64: {
      return velox::TypePtr(velox::DOUBLE());
    }
    case io::substrait::Type::kTimestamp: {
      return velox::TypePtr(velox::TIMESTAMP());
    }
    case io::substrait::Type::kMap: {
      velox::TypePtr keyType = substraitTypeToVelox(sType.map().key());
      velox::TypePtr valueType = substraitTypeToVelox(sType.map().value());
      return velox::TypePtr(velox::MAP(keyType, valueType));
    }
    case io::substrait::Type::kList: {
      velox::TypePtr listType = substraitTypeToVelox(sType.list().type());
      return velox::TypePtr(velox::ARRAY(listType));
    }
    case io::substrait::Type::kDate:
    case io::substrait::Type::kTime:
    case io::substrait::Type::kIntervalDay:
    case io::substrait::Type::kIntervalYear:
    case io::substrait::Type::kTimestampTz:
    case io::substrait::Type::kStruct:
    case io::substrait::Type::kUserDefined:
    case io::substrait::Type::kUuid:
    default:
      throw std::runtime_error(
          "Unsupported type " + std::to_string(sType.kind_case()));

      // ROW  UNKNOWN FUNCTION  OPAQUE(using NativeType = std::shared_prt<void>)
      // INVALID(void)
  }
}

std::shared_ptr<FilterNode> SubstraitVeloxConvertor::transformSFilter(
    const io::substrait::Rel& sRel,
    int depth) {
  const io::substrait::FilterRel& sFilter = sRel.filter();
  if (!sFilter.has_condition()) {
    return std::make_shared<FilterNode>(
        std::to_string(depth), nullptr, fromSubstraitIR(sRel, depth + 1));
  }
  const io::substrait::Expression& sExpr = sFilter.condition();
  return std::make_shared<FilterNode>(
      std::to_string(depth),
      transformSExpr(sExpr, sFilter.common()),
      fromSubstraitIR(sRel, depth + 1));
}

std::shared_ptr<const ITypedExpr>
SubstraitVeloxConvertor::transformSLiteralExpr(
    const io::substrait::Expression_Literal& sLiteralExpr) {
  switch (sLiteralExpr.literal_type_case()) {
    case io::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
      // TODO
      return std::make_shared<ConstantTypedExpr>(
          velox::variant(sLiteralExpr.decimal()));
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kString: {
      // TODO lets not construct the expression everywhere ay
      return std::make_shared<ConstantTypedExpr>(
          velox::variant(sLiteralExpr.string()));
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
      return std::make_shared<ConstantTypedExpr>(
          velox::variant(sLiteralExpr.boolean()));
    }
    default:
      throw std::runtime_error(
          std::to_string(sLiteralExpr.literal_type_case()));
  }
}

std::shared_ptr<const ITypedExpr> SubstraitVeloxConvertor::transformSExpr(
    const io::substrait::Expression& sExpr,
    io::substrait::RelCommon sRelCommon) {
  switch (sExpr.rex_type_case()) {
    case io::substrait::Expression::RexTypeCase::kLiteral: {
      auto slit = sExpr.literal();
      std::shared_ptr<const ITypedExpr> sConstant = transformSLiteralExpr(slit);
      return sConstant;
    }
    case io::substrait::Expression::RexTypeCase::kSelection: {
      if (!sExpr.selection().has_direct_reference() ||
          !sExpr.selection().direct_reference().has_struct_field()) {
        throw std::runtime_error(
            "Can only have direct struct references in selections");
      }

      // TODO need to confirm the index, use which or just the 0, i think
      // there is only one column in this case, or for sort order.
      // the cloumn
      auto outId = sExpr.selection().direct_reference().struct_field().field();
      // the outputinfo of this column  in the emit
      io::substrait::Type_NamedStruct* outputInfo =
          sRelCommon.mutable_emit()->mutable_output_mapping(outId);
      // the index in the output emit
      int64_t colId = outputInfo->index(outId);
      io::substrait::Type stype = outputInfo->struct_().types(colId);
      std::string sName = outputInfo->names(colId);
      velox::TypePtr vType = substraitTypeToVelox(stype);
      return std::make_shared<FieldAccessTypedExpr>(vType, sName);
    }
    case io::substrait::Expression::RexTypeCase::kScalarFunction: {
      // TODO the type is the function parameters' types, now all set to boolean
      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sRelCommon));
      }
      std::string function_name =
          FindFunction(sExpr.scalar_function().id().id());
      //  and or  try concatrow
      if (function_name != "if" && function_name != "switch") {
        return std::make_shared<CallTypedExpr>(
            velox::BOOLEAN(), children, function_name);
      }
    }
    case io::substrait::Expression::RexTypeCase::kIfThen: {
      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sRelCommon));
      }
      return std::make_shared<velox::core::CallTypedExpr>(
          velox::BOOLEAN(), move(children), "if");
    }
    case io::substrait::Expression::RexTypeCase::kSwitchExpression: {
      // TODO switch
      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sRelCommon));
      }
      return std::make_shared<velox::core::CallTypedExpr>(
          velox::BOOLEAN(), move(children), "switch");
    }
    default:
      throw std::runtime_error(
          "Unsupported expression type " +
          std::to_string(sExpr.rex_type_case()));
  }
};

std::shared_ptr<PartitionedOutputNode>
SubstraitVeloxConvertor::transformSDistribute(
    const io::substrait::Plan& sPlan,
    int depth) {}

const velox::RowTypePtr SubstraitVeloxConvertor::sNamedStructToVRowTypePtr(
    io::substrait::Type_NamedStruct sNamedStruct) {
  auto sIndex = sNamedStruct.index();
  std::vector<std::string> vNames;
  std::vector<velox::TypePtr> vTypes;

  for (auto index : sIndex) {
    const io::substrait::Type& sType = sNamedStruct.struct_().types(index);
    velox::TypePtr vType = substraitTypeToVelox(sType);
    vNames.push_back(sNamedStruct.names(index));
    vTypes.push_back(vType);
  }
  return ROW(std::move(vNames), std::move(vTypes));
}

std::shared_ptr<const ITypedExpr> SubstraitVeloxConvertor::parseExpr(
    const std::string& text,
    std::shared_ptr<const velox::RowType> vRowType) {
  auto untyped = velox::parse::parseExpr(text);
  return Expressions::inferTypes(untyped, vRowType, nullptr);
}

template <TypeKind KIND>
void setCellFromVariantByKind(
    const VectorPtr& column,
    vector_size_t row,
    const velox::variant& value) {
  using T = typename TypeTraits<KIND>::NativeType;

  auto flatVector = column->as<FlatVector<T>>();
  flatVector->set(row, value.value<T>());
}

template <>
void setCellFromVariantByKind<TypeKind::VARBINARY>(
    const VectorPtr& /*column*/,
    vector_size_t /*row*/,
    const velox::variant& value) {
  throw std::invalid_argument("Return of VARBINARY data is not supported");
}

template <>
void setCellFromVariantByKind<TypeKind::VARCHAR>(
    const VectorPtr& column,
    vector_size_t row,
    const velox::variant& value) {
  auto values = column->as<FlatVector<StringView>>();
  values->set(row, StringView(value.value<Varchar>()));
}

void setCellFromVariant(
    const RowVectorPtr& data,
    vector_size_t row,
    vector_size_t column,
    const velox::variant& value) {
  auto columnVector = data->childAt(column);
  if (value.isNull()) {
    columnVector->setNull(row, true);
    return;
  }
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setCellFromVariantByKind,
      columnVector->typeKind(),
      columnVector,
      row,
      value);
}

std::shared_ptr<PlanNode> SubstraitVeloxConvertor::transformSRead(
    const io::substrait::Rel& sRel,
    int depth) {
  const io::substrait::ReadRel& sRead = sRel.read();
  const std::shared_ptr<const velox::RowType> vOutputType =
      sNamedStructToVRowTypePtr(sRead.base_schema());

  // TODO need to add the impl of type local_files

  if (sRead.has_filter()) {
    return std::make_shared<FilterNode>(
        std::to_string(depth),
        transformSExpr(sRead.filter(), sRead.common()),
        fromSubstraitIR(sRel, depth + 1));
  }

  if (sRead.has_projection()) {
    std::vector<std::shared_ptr<const ITypedExpr>> expressions;
    for (auto& sproj : sRead.projection().select().struct_items()) {
      // TODO make sure nothing else is in there
      expressions.push_back(
          parseExpr(std::to_string(sproj.field()), vOutputType));
    }

    return std::make_shared<ProjectNode>(
        std::to_string(depth),
        vOutputType->names(),
        expressions,
        fromSubstraitIR(sRel, depth + 1));
  }

  if (sRead.has_named_table()) {
    std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>
        assignments;
    for (auto& name : vOutputType->names()) {
      std::shared_ptr<velox::connector::ColumnHandle> colHandle =
          std::make_shared<velox::connector::hive::HiveColumnHandle>(
              name,
              velox::connector::hive::HiveColumnHandle::ColumnType::kRegular);
      assignments.insert({name, colHandle});
    }

    std::shared_ptr<velox::connector::ConnectorTableHandle> tableHandle =
        std::make_shared<velox::connector::hive::HiveTableHandle>(
            true, velox::connector::hive::SubfieldFilters{}, nullptr);

    return std::make_shared<TableScanNode>(
        std::to_string(depth), vOutputType, tableHandle, assignments);
  }

  if (sRead.has_virtual_table()) {
    bool parallelizable = false;
    velox::memory::MemoryPool* pool_;

    int64_t numRows = sRead.virtual_table().values_size();
    int64_t numColumns = vOutputType->size();
    std::vector<velox::VectorPtr> vectors;
    vectors.reserve(numColumns);

    for (int64_t i = 0; i < numColumns; ++i) {
      auto base =
          velox::BaseVector::create(vOutputType->childAt(i), numRows, pool_);
      vectors.emplace_back(base);
    }

    auto rowVector = std::make_shared<RowVector>(
        pool_, vOutputType, BufferPtr(), numRows, std::move(vectors), 0);

    for (int64_t row = 0; row < numRows; ++row) {
      io::substrait::Expression_Literal_Struct value =
          sRead.virtual_table().values(row);
      numColumns = value.kFieldsFieldNumber;
      for (int64_t column = 0; column < numColumns; ++column) {
        io::substrait::Expression_Literal field = value.fields(column);

        auto expr = transformSLiteralExpr(field);

        if (auto constantExpr =
                std::dynamic_pointer_cast<const ConstantTypedExpr>(expr)) {
          if (!constantExpr->hasValueVector()) {
            setCellFromVariant(rowVector, row, column, constantExpr->value());
          } else {
            VELOX_UNSUPPORTED(
                "Values node with complex type values is not supported yet");
          }
        } else {
          VELOX_FAIL("Expected constant expression");
        }
      }
    }

    return std::make_shared<ValuesNode>(
        std::to_string(depth),
        std::vector<RowVectorPtr>{rowVector},
        parallelizable);
  }
}

std::shared_ptr<ProjectNode> SubstraitVeloxConvertor::transformSProject(
    const io::substrait::Rel& sRel,
    int depth) {
  const io::substrait::ProjectRel& sProj = sRel.project();
  std::vector<std::shared_ptr<const ITypedExpr>> vExpressions;
  std::vector<std::string> names;

  for (auto& sExpr : sProj.expressions()) {
    vExpressions.push_back(transformSExpr(sExpr, sProj.common()));
  }

  for (auto index : sProj.common().emit().output_mapping(depth).index()) {
    names.push_back(sProj.common().emit().output_mapping(depth).names(index));
  }
  return std::make_shared<ProjectNode>(
      std::to_string(depth),
      names,
      vExpressions,
      fromSubstraitIR(sProj.input(), depth + 1));
}

std::shared_ptr<AggregationNode> SubstraitVeloxConvertor::transformSAggregate(
    const io::substrait::Rel& sRel,
    int depth) {
  AggregationNode::Step step;
  // TODO now is set false, need to add additional info to check this.
  bool ignoreNullKeys = false;
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> aggregateMasks;
  std::shared_ptr<const FieldAccessTypedExpr> aggregateMask;
  std::vector<std::shared_ptr<const CallTypedExpr>> aggregates;
  std::vector<std::string> aggregateNames;
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> groupingKeys;
  std::shared_ptr<const FieldAccessTypedExpr> groupingKey;

  const io::substrait::AggregateRel& sagg = sRel.aggregate();
  switch (sagg.phase()) {
    case io::substrait::Expression_AggregationPhase::
        Expression_AggregationPhase_INITIAL_TO_INTERMEDIATE: {
      step = AggregationNode::Step::kPartial;
      break;
    }
    case io::substrait::Expression_AggregationPhase::
        Expression_AggregationPhase_INTERMEDIATE_TO_RESULT: {
      step = AggregationNode::Step::kFinal;
      break;
    }
    case io::substrait::Expression_AggregationPhase::
        Expression_AggregationPhase_INTERMEDIATE_TO_INTERMEDIATE: {
      step = AggregationNode::Step::kIntermediate;
      break;
    }
    case io::substrait::Expression_AggregationPhase::
        Expression_AggregationPhase_INITIAL_TO_RESULT: {
      step = AggregationNode::Step::kSingle;
      break;
    }
    default:
      VELOX_UNSUPPORTED("Unsupported aggregation step");
  }

  // TODO need to confirm sgroup.input_fields is one column or not?
  // TODO groupings and input_fields should only one be repeated, substraitRel
  // have both, need confirm
  int64_t sgroupId = 0;
  // multi grouping set
  for (auto& sgroup : sagg.groupings()) {
    std::string name;
    // assume only 1?
    for (int64_t colIndex : sgroup.input_fields()) {
      int64_t index =
          sagg.common().emit().output_mapping(sgroupId).index(colIndex);
      name = sagg.common().emit().output_mapping(sgroupId).names(index);
      io::substrait::Type stype =
          sagg.common().emit().output_mapping(sgroupId).struct_().types(index);
      velox::TypePtr vType = substraitTypeToVelox(stype);
      groupingKey = std::make_shared<FieldAccessTypedExpr>(vType, name);
      groupingKeys.push_back(groupingKey);
    }
    sgroupId++;
  }
  // for velox  sum(c) is ok, but sum(c + d) is not.
  for (auto& smeas : sagg.measures()) {
    std::vector<std::shared_ptr<const ITypedExpr>> children;
    std::string out_name;
    std::string function_name = FindFunction(smeas.measure().id().id());
    out_name = function_name;
    // AggregateFunction.args
    for (const io::substrait::Expression& sarg : smeas.measure().args()) {
      std::shared_ptr<const ITypedExpr> vexpr =
          transformSExpr(sarg, sagg.common());
      children.push_back(vexpr);
      out_name += vexpr->toString();
    }

    aggregateNames.push_back(out_name);
    aggregates.push_back(std::make_shared<const CallTypedExpr>(
        substraitTypeToVelox(smeas.measure().output_type()),
        move(children),
        function_name));

    // TODO need to check with substrait community
    // For each measure, an optional boolean input column that is used to mask
    // out rows for this particular measure.
    /*    for (int index : smeas.mask().index()) { // only 1?
          io::substrait::Type stype = smeas.mask().struct_().types(index);
          aggregateMask = std::make_shared<FieldAccessTypedExpr>(
              substraitTypeToVelox(stype), smeas.mask().names(index));
        }
        aggregateMasks.push_back(aggregateMask);*/
  }

  return std::make_shared<AggregationNode>(
      std::to_string(depth),
      step,
      groupingKeys,
      aggregateNames,
      aggregates,
      aggregateMasks,
      ignoreNullKeys,
      fromSubstraitIR(sagg.input(), depth + 1));
}

std::shared_ptr<OrderByNode> SubstraitVeloxConvertor::transformSSort(
    const io::substrait::Rel& sRel,
    int depth) {
  std::vector<OrderByNode> velox_nodes;
  const io::substrait::SortRel& sSort = sRel.sort();

  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys;
  std::vector<SortOrder> sortingOrders;
  bool isPartial;

  isPartial = sSort.common().distribution().d_type() == 0 ? true : false;

  // The supported orders are: ascending nulls first, ascending nulls last,
  // descending nulls first, descending nulls last
  for (const io::substrait::Expression_SortField& sOrderField : sSort.sorts()) {
    // TODO check whether  ssort.common() need to be the node output before
    const io::substrait::Expression sExpr = sOrderField.expr();
    std::shared_ptr<const ITypedExpr> sortingKey =
        transformSExpr(sExpr, sSort.common());
    auto constSortKey =
        std::dynamic_pointer_cast<const FieldAccessTypedExpr>(sortingKey);
    sortingKeys.push_back(constSortKey);

    switch (sOrderField.formal()) {
      case io::substrait::Expression_SortField_SortType::
          Expression_SortField_SortType_ASC_NULLS_FIRST:
        sortingOrders.push_back(SortOrder(true, true));
      case io::substrait::Expression_SortField_SortType::
          Expression_SortField_SortType_ASC_NULLS_LAST:
        sortingOrders.push_back(SortOrder(true, false));
      case io::substrait::Expression_SortField_SortType::
          Expression_SortField_SortType_DESC_NULLS_FIRST:
        sortingOrders.push_back(SortOrder(false, true));
      case io::substrait::Expression_SortField_SortType::
          Expression_SortField_SortType_DESC_NULLS_LAST:
        sortingOrders.push_back(SortOrder(false, false));
      default:
        throw std::runtime_error(
            "Unsupported ordering " + std::to_string(sOrderField.formal()));
    }
  }
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
      constSortingKeys = sortingKeys;
  const std::vector<SortOrder>& constSortingOrders = sortingOrders;
  return std::make_shared<OrderByNode>(
      std::to_string(depth),
      constSortingKeys,
      constSortingOrders,
      isPartial,
      fromSubstraitIR(sSort.input(), depth + 1));
}

// ==================   Produce substrait rel    ==================
// ==================   Public APIs   ==================
/**
 * Source is 1st pos of inserted tree
 * @param planNode
 * @return
 */

void SubstraitVeloxConvertor::toSubstraitIR(
    std::shared_ptr<const PlanNode> vPlan,
    io::substrait::Plan sPlan) {
  // TODO register function mapping
  // Assume only accepts a single plan fragment
  io::substrait::Rel* sRel = sPlan.add_relations();
  toSubstraitIR(vPlan, sRel);
}

// =========   Private APIs for making Velox operators   =========
/**
 * Flat output node with source node
 * @param planNode
 * @param srel
 */
void SubstraitVeloxConvertor::toSubstraitIR(
    std::shared_ptr<const PlanNode> vPlanNode,
    io::substrait::Rel* sRel) {
  // auto nextNode = vPlanNode->sources()[0];
  io::substrait::RelCommon* relCommon;
  if (auto filterNode =
          std::dynamic_pointer_cast<const FilterNode>(vPlanNode)) {
    auto sFilterRel = sRel->mutable_filter();
    transformVFilter(filterNode, sFilterRel);
    relCommon = sFilterRel->mutable_common();
  }
  if (auto aggNode =
          std::dynamic_pointer_cast<const AggregationNode>(vPlanNode)) {
    auto sAggRel = sRel->mutable_aggregate();
    transformVAgg(aggNode, sAggRel);
    relCommon = sAggRel->mutable_common();
  }
  if (auto vValuesNode =
          std::dynamic_pointer_cast<const ValuesNode>(vPlanNode)) {
    io::substrait::ReadRel* sReadRel = sRel->mutable_read();

    transformVValuesNode(vValuesNode, sReadRel);
    relCommon = sReadRel->mutable_common();
  }
  if (auto vProjNode =
          std::dynamic_pointer_cast<const ProjectNode>(vPlanNode)) {
    io::substrait::ProjectRel* sProjRel = sRel->mutable_project();
    transformVProjNode(vProjNode, sProjRel);
    relCommon = sProjRel->mutable_common();
  }
  if (auto partitionedOutputNode =
          std::dynamic_pointer_cast<const PartitionedOutputNode>(vPlanNode)) {
    io::substrait::DistributeRel* dRel = sRel->mutable_distribute();
    dRel->set_partitioncount(partitionedOutputNode->numPartitions());
    transformVPartitionedOutputNode(partitionedOutputNode, dRel);
  }

  // For output node, needs to put distribution info into its source node's
  // relcommon
  // this part can be used to check if partition is enable.
  if (auto partitionedOutputNode =
          std::dynamic_pointer_cast<const PartitionedOutputNode>(vPlanNode)) {
    relCommon->mutable_distribution()->set_d_type(
        io::substrait::RelCommon_Distribution_DISTRIBUTION_TYPE::
            RelCommon_Distribution_DISTRIBUTION_TYPE_PARTITIONED);
  } else {
    relCommon->mutable_distribution()->set_d_type(
        io::substrait::RelCommon_Distribution_DISTRIBUTION_TYPE::
            RelCommon_Distribution_DISTRIBUTION_TYPE_SINGLETON);
  }
  //    auto d_field = relCommon->mutable_distribution()->mutable_d_field;
}

io::substrait::Type_NamedStruct*
SubstraitVeloxConvertor::vRowTypePtrToSNamedStruct(
    velox::RowTypePtr vRow,
    io::substrait::Type_NamedStruct* sNamedStruct) {
  int64_t vSize = vRow->size();
  std::vector<std::string> vNames = vRow->names();
  std::vector<std::shared_ptr<const Type>> vTypes = vRow->children();

  for (int64_t i = 0; i < vSize; ++i) {
    std::string vName = vNames.at(i);
    std::shared_ptr<const Type> vType = vTypes.at(i);
    sNamedStruct->add_index(i);
    sNamedStruct->add_names(vName);
    io::substrait::Type* sStruct = sNamedStruct->mutable_struct_()->add_types();

    veloxTypeToSubstrait(vType, sStruct);
  }

  return sNamedStruct;
}

void SubstraitVeloxConvertor::transformVValuesNode(
    std::shared_ptr<const ValuesNode> vValuesNode,
    io::substrait::ReadRel* sReadRel) {
  const RowTypePtr vOutPut = vValuesNode->outputType();

  io::substrait::ReadRel_VirtualTable* sVirtualTable =
      sReadRel->mutable_virtual_table();

  io::substrait::Type_NamedStruct* sBaseSchema =
      sReadRel->mutable_base_schema();
  vRowTypePtrToSNamedStruct(vOutPut, sBaseSchema);


  io::substrait::RelCommon_Emit* sOutputEmit =
      sReadRel->mutable_common()->mutable_emit();

  io::substrait::Type_NamedStruct* sOutputMapping =
      sOutputEmit->add_output_mapping();
  vRowTypePtrToSNamedStruct(vOutPut, sOutputMapping);

  const PlanNodeId id = vValuesNode->id();
  // sread.virtual_table().values_size(); multi rows
  int64_t numRows = vValuesNode->values().size();
  // should be the same value.kFieldsFieldNumber  = vOutputType->size();
  int64_t numColumns;
  // multi rows, each row is a RowVectorPrt
  io::substrait::Expression_Literal_Struct* sLitValue =
      sVirtualTable->add_values();

  for (int64_t row = 0; row < numRows; ++row) {
    // mutable_values(row);
    // the specfic row
    RowVectorPtr rowValue = vValuesNode->values().at(row);
    // the column numbers in the specfic row.
    numColumns = rowValue->childrenSize();

    for (int64_t column = 0; column < numColumns; ++column) {
      io::substrait::Expression_Literal* sField = sLitValue->add_fields();

      auto children = rowValue->childAt(column);
      // should be the same with rowValue->type();
      std::shared_ptr<const Type> childType = children->type();

      auto childernValue = children->values();

      switch (childType->kind()) {
        case velox::TypeKind::BOOLEAN: {
          sField->set_boolean(*childernValue->asMutable<bool>());
          break;
        }
        case velox::TypeKind::TINYINT: {
          sField->set_i8(*childernValue->asMutable<int8_t>());
          break;
        }
        case velox::TypeKind::SMALLINT: {
          sField->set_i16(*childernValue->asMutable<int16_t>());
          break;
        }
        case velox::TypeKind::INTEGER: {
          sField->set_i32(*childernValue->asMutable<int32_t>());
          break;
        }
        case velox::TypeKind::BIGINT: {
          sField->set_i64(*childernValue->asMutable<int64_t>());
          break;
        }
        case velox::TypeKind::REAL: {
          sField->set_fp32(*childernValue->asMutable<float_t>());
          break;
        }
        case velox::TypeKind::DOUBLE: {
          sField->set_fp64(*childernValue->asMutable<double_t>());
          break;
        }
        case velox::TypeKind::VARCHAR: {
          sField->set_var_char(*childernValue->asMutable<std::string>());
          break;
        }
        default:
          throw std::runtime_error(
              "Unsupported type " + std::string(childType->kindName()));
      }
    }
  }
}

void SubstraitVeloxConvertor::transformVProjNode(
    std::shared_ptr<const ProjectNode> vProjNode,
    io::substrait::ProjectRel* sProjRel) {
  const PlanNodeId vId = vProjNode->id();
  std::vector<std::string> vNames = vProjNode->names();
  std::vector<std::shared_ptr<const ITypedExpr>> vProjections =
      vProjNode->projections();
  std::shared_ptr<const PlanNode> vSource = vProjNode->sources()[0];
  toSubstraitIR(vSource, sProjRel->mutable_input());

  int64_t vProjectionSize = vProjections.size();
  for (int64_t i = 0; i < vProjectionSize; i++) {
    std::shared_ptr<const ITypedExpr>& vExpr = vProjections.at(i);
    io::substrait::Expression* sExpr = sProjRel->add_expressions();
    transformVExpr(sExpr, sProjRel->mutable_common(), i, vExpr);
  }

  io::substrait::Type_NamedStruct* sNames =
      sProjRel->mutable_common()->mutable_emit()->add_output_mapping();
  int64_t vNameSize = vNames.size();
  for (int64_t i = 0; i < vNameSize; i++) {
    sNames->add_index(i);
    sNames->add_names(vNames[i]);
  }
  std::cout << sNames << std::endl;
  return;
}

uint64_t SubstraitVeloxConvertor::registerSFunction(std::string name) {
  if (function_map.find(name) == function_map.end()) {
    auto function_id = last_function_id++;
    auto sfun = plan.add_mappings()->mutable_function_mapping();
    sfun->mutable_extension_id()->set_id(42);
    sfun->mutable_function_id()->set_id(function_id);
    sfun->set_index(function_id);
    sfun->set_name(name);

    function_map[name] = function_id;
  }
  return function_map[name];
}

void SubstraitVeloxConvertor::transformVPartitionFunc(
    io::substrait::DistributeRel* sDistRel,
    std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode) {
  std::shared_ptr<PartitionFunction> factory =
      vPartitionedOutputNode->partitionFunctionFactory()(
          vPartitionedOutputNode->numPartitions());

  if (auto f = std::dynamic_pointer_cast<velox::exec::HashPartitionFunction>(
          factory)) {
    auto func_id = registerSFunction("HashPartitionFunction");
    auto scala_function =
        sDistRel->mutable_d_field()->mutable_expr()->mutable_scalar_function();
    scala_function->mutable_id()->set_id(func_id);
    // TODO: add parameters
    //  //velox std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
    //  to substrait selection FieldReference
    auto keys = vPartitionedOutputNode->keys();
    // TODO  TransformVExpr(velox::Expression &vexpr, substrait::Expression
    // &sexpr)
    //  velox FieldAccessTypedExpr to substrait selection.
    auto outputInfo = sDistRel->common().emit().output_mapping();
    // scala_function->add_args()->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(partitionedOutputNode->keys());

  } else if (
      auto f =
          std::dynamic_pointer_cast<velox::exec::RoundRobinPartitionFunction>(
              factory)) {
    auto func_id = registerSFunction("RoundRobinPartitionFunction");
    auto scala_function =
        sDistRel->mutable_d_field()->mutable_expr()->mutable_scalar_function();
    scala_function->mutable_id()->set_id(func_id);
    // TODO add keys

  } else if (
      auto f = std::dynamic_pointer_cast<
          velox::connector::hive::HivePartitionFunction>(factory)) {
    auto func_id = registerSFunction("HivePartitionFunction");
    auto scala_function =
        sDistRel->mutable_d_field()->mutable_expr()->mutable_scalar_function();
    scala_function->mutable_id()->set_id(func_id);
    // TODO add keys
  }
}

void SubstraitVeloxConvertor::transformVPartitionedOutputNode(
    std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode,
    io::substrait::DistributeRel* sDistRel) {
  if (vPartitionedOutputNode->isBroadcast()) {
    sDistRel->set_type(io::substrait::DistributeRel_DistributeType::
                           DistributeRel_DistributeType_boradcast);
  } else {
    sDistRel->set_type(io::substrait::DistributeRel_DistributeType::
                           DistributeRel_DistributeType_scatter);
  }

  // Transform distribution function
  transformVPartitionFunc(sDistRel, vPartitionedOutputNode);

  // Handle emit for output
  std::vector<std::shared_ptr<const io::substrait::Type>> sTypes;
  const RowTypePtr vOutPut = vPartitionedOutputNode->outputType();
  std::vector<std::string> names = vOutPut->names();
  std::vector<std::shared_ptr<const velox::Type>> vTypes = vOutPut->children();

  int64_t vOutSize = vOutPut->size();
  io::substrait::RelCommon_Emit* sOutputEmit =
      sDistRel->mutable_common()->mutable_emit();

  for (int64_t i = 0; i < vOutSize; i++) {
    io::substrait::Type_NamedStruct* sOutputMapping =
        sOutputEmit->mutable_output_mapping(i);
    vRowTypePtrToSNamedStruct(vOutPut, sOutputMapping);
  }

  //  Back to handle source node
  //  TODO miss  the parameter  bool replicateNullsAndAny
  toSubstraitIR(
      vPartitionedOutputNode->sources()[0], sDistRel->mutable_input());
}

void SubstraitVeloxConvertor::transformVFilter(
    std::shared_ptr<const FilterNode> vFilter,
    io::substrait::FilterRel* sFilter) {
  //   Construct substrait expr
  transformVExpr(
      sFilter->mutable_condition(),
      sFilter->mutable_common(),
      0,
      vFilter->filter());

  //   Build source
  toSubstraitIR(vFilter->sources()[0], sFilter->mutable_input());
}

void SubstraitVeloxConvertor::transformVAgg(
    std::shared_ptr<const AggregationNode> vAgg,
    io::substrait::AggregateRel* sAgg) {
  // TODO
  //   Construct substrait expr
  //  transformVExpr(sfilter.mutable_condition(), vagg->filter());

  //   Build source
  toSubstraitIR(vAgg->sources()[0], sAgg->mutable_input());
}

// Private APIs for making expressions
void SubstraitVeloxConvertor::transformVExpr(
    io::substrait::Expression* sExpr,
    io::substrait::RelCommon* sRelCommon,
    int64_t index,
    const std::shared_ptr<const ITypedExpr>& vExpr) {
  // TODO
  if (auto vConstantExpr =
          std::dynamic_pointer_cast<const ConstantTypedExpr>(vExpr)) {
    // Literal
    io::substrait::Expression_Literal* sLiteralExpr = sExpr->mutable_literal();
    // TODO remove this after checked.
    std::cout << vConstantExpr->toString();
    transformVConstantExpr(vConstantExpr->value(), sLiteralExpr);
    return;
  } else if (
      auto vCallTypeExpr =
          std::dynamic_pointer_cast<const CallTypedExpr>(vExpr)) {
    // different by function names.
    // kIfThen
    // kScalarFunction
    // kSwitchExpression
  } else if (
      auto vFieldExpr =
          std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vExpr)) {
    // kSelection
    const std::shared_ptr<const Type> vExprType = vFieldExpr->type();
    std::string vExprName = vFieldExpr->name();

    io::substrait::ReferenceSegment_StructField* sDirectStruct =
        sExpr->mutable_selection()
            ->mutable_direct_reference()
            ->mutable_struct_field();

    // TODO for now , it's multi output_mapping and one index in the
    // output_mapping
    /*//another case is  (one table and multi columns ??),  one output_mapping
     * and multi index in the output_mapping*/
    sDirectStruct->set_field(index);
    int64_t sId = sDirectStruct->field();
    io::substrait::Type_NamedStruct* sOutMapping =
        sRelCommon->mutable_emit()->add_output_mapping();

    sOutMapping->add_index(sId);
    sOutMapping->add_names(vExprName);

    io::substrait::Type* sOutMappingStructType =
        sOutMapping->mutable_struct_()->add_types();

    veloxTypeToSubstrait(vExprType, sOutMappingStructType);
    return;

  } else {
    throw std::runtime_error(
        "Unsupport Expr " + vExpr->toString() + "in Substrait");
    return;
  }
}

void SubstraitVeloxConvertor::transformVConstantExpr(
    const velox::variant& vConstExpr,
    io::substrait::Expression_Literal* sLiteralExpr) {
  switch (vConstExpr.kind()) {
    case velox::TypeKind::DOUBLE: {
      // TODO
      sLiteralExpr->mutable_decimal()->push_back(1);
      break;
    }
    case velox::TypeKind::VARCHAR: {
      std::basic_string<char> vCharValue = vConstExpr.value<Varchar>();
      sLiteralExpr->set_allocated_string(&vCharValue);
      break;
    }
    default:
      throw std::runtime_error(
          "Unsupported constant Type" + mapTypeKindToName(vConstExpr.kind()));
  }
}

io::substrait::Type SubstraitVeloxConvertor::veloxTypeToSubstrait(
    const velox::TypePtr& vType,
    io::substrait::Type* sType) {
  switch (vType->kind()) {
    case velox::TypeKind::BOOLEAN: {
      sType->set_allocated_bool_(new io::substrait::Type_Boolean());
      return *sType;
    }
    case velox::TypeKind::TINYINT: {
      sType->set_allocated_i8(new io::substrait::Type_I8());
      return *sType;
    }
    case velox::TypeKind::SMALLINT: {
      sType->set_allocated_i16(new io::substrait::Type_I16());
      return *sType;
    }
    case velox::TypeKind::INTEGER: {
      sType->set_allocated_i32(new io::substrait::Type_I32());
      return *sType;
    }
    case velox::TypeKind::BIGINT: {
      sType->set_allocated_i64(new io::substrait::Type_I64());
      return *sType;
    }
    case velox::TypeKind::REAL: {
      sType->set_allocated_fp32(new io::substrait::Type_FP32());
      return *sType;
    }
    case velox::TypeKind::DOUBLE: {
      sType->set_allocated_fp64(new io::substrait::Type_FP64());
      return *sType;
    }
    case velox::TypeKind::VARCHAR: {
      sType->set_allocated_varchar(new io::substrait::Type_VarChar());
      return *sType;
    }
    case velox::TypeKind::VARBINARY: {
      sType->set_allocated_binary(new io::substrait::Type_Binary());
      return *sType;
    }
    case velox::TypeKind::TIMESTAMP: {
      sType->set_allocated_timestamp(new io::substrait::Type_Timestamp());
      return *sType;
    }
    case velox::TypeKind::ARRAY: {
      io::substrait::Type_List* sTList = new io::substrait::Type_List();
      const std::shared_ptr<const Type> vArrayType =
          vType->asArray().elementType();
      io::substrait::Type sListType =
          veloxTypeToSubstrait(vArrayType, sTList->mutable_type());

      sType->set_allocated_list(sTList);
      return *sType;
    }
    case velox::TypeKind::MAP: {
      io::substrait::Type_Map* sMap = new io::substrait::Type_Map();
      const std::shared_ptr<const Type> vMapKeyType = vType->asMap().keyType();
      const std::shared_ptr<const Type> vMapValueType =
          vType->asMap().valueType();

      veloxTypeToSubstrait(vMapKeyType, sMap->mutable_key());
      veloxTypeToSubstrait(vMapValueType, sMap->mutable_value());

      sType->set_allocated_map(sMap);
      return *sType;
    }
    case velox::TypeKind::UNKNOWN:
    case velox::TypeKind::FUNCTION:
    case velox::TypeKind::OPAQUE:
    case velox::TypeKind::INVALID:
    default:
      throw std::runtime_error(
          "Unsupported type " + std::string(vType->kindName()));
  }
}

} // namespace facebook::velox
