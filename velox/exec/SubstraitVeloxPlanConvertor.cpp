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

#include "SubstraitVeloxPlanConvertor.h"
#include "GlobalCommonVariable.h"

namespace facebook::velox {

// Velox Plan to Substrait
void VeloxToSubstraitPlanConvertor::veloxToSubstraitIR(
    std::shared_ptr<const PlanNode> vPlan,
    io::substrait::Plan& sPlan) {
  // Assume only accepts a single plan fragment
  io::substrait::Rel* sRel = sPlan.add_relations();
  veloxToSubstraitIR(vPlan, sRel);
}

void VeloxToSubstraitPlanConvertor::veloxToSubstraitIR(
    std::shared_ptr<const PlanNode> vPlanNode,
    io::substrait::Rel* sRel) {
  // auto nextNode = vPlanNode->sources()[0];
  io::substrait::RelCommon* relCommon;
  if (auto filterNode =
          std::dynamic_pointer_cast<const FilterNode>(vPlanNode)) {
    auto sFilterRel = sRel->mutable_filter();
    transformVFilter(filterNode, sFilterRel, sGlobalMapping_);
    relCommon = sFilterRel->mutable_common();
  }
  if (auto aggNode =
          std::dynamic_pointer_cast<const AggregationNode>(vPlanNode)) {
    auto sAggRel = sRel->mutable_aggregate();
    transformVAggregateNode(aggNode, sAggRel, sGlobalMapping_);
    relCommon = sAggRel->mutable_common();
  }
  if (auto vValuesNode =
          std::dynamic_pointer_cast<const ValuesNode>(vPlanNode)) {
    io::substrait::ReadRel* sReadRel = sRel->mutable_read();
    transformVValuesNode(vValuesNode, sReadRel);
    relCommon = sReadRel->mutable_common();

    sGlobalMapping_->MergeFrom(*sReadRel->mutable_base_schema());
  }
  if (auto vProjNode =
          std::dynamic_pointer_cast<const ProjectNode>(vPlanNode)) {
    io::substrait::ProjectRel* sProjRel = sRel->mutable_project();
    transformVProjNode(vProjNode, sProjRel, sGlobalMapping_);
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
  // TODO this is for debug
  LOG(INFO) << "the final " << std::endl;
  sRel->PrintDebugString();
  //    auto d_field = relCommon->mutable_distribution()->mutable_d_field;
}
void VeloxToSubstraitPlanConvertor::transformVFilter(
    std::shared_ptr<const FilterNode> vFilterNode,
    io::substrait::FilterRel* sFilterRel,
    io::substrait::Type_NamedStruct* sGlobalMapping) {
  const PlanNodeId vId = vFilterNode->id();
  std::shared_ptr<const PlanNode> vSource = vFilterNode->sources()[0];
  std::shared_ptr<const ITypedExpr> vFilterCondition = vFilterNode->filter();

  io::substrait::Rel* sFilterInput = sFilterRel->mutable_input();
  io::substrait::Expression* sFilterCondition = sFilterRel->mutable_condition();
  //   Build source
  veloxToSubstraitIR(vSource, sFilterInput);
  //   Construct substrait expr
  v2SExprConvertor.transformVExpr(
      sFilterCondition, vFilterCondition, sGlobalMapping);
}

void VeloxToSubstraitPlanConvertor::transformVValuesNode(
    std::shared_ptr<const ValuesNode> vValuesNode,
    io::substrait::ReadRel* sReadRel) {
  const RowTypePtr vOutPut = vValuesNode->outputType();

  io::substrait::ReadRel_VirtualTable* sVirtualTable =
      sReadRel->mutable_virtual_table();

  io::substrait::Type_NamedStruct* sBaseSchema =
      sReadRel->mutable_base_schema();
  v2STypeConvertor.vRowTypePtrToSNamedStruct(vOutPut, sBaseSchema);

  const PlanNodeId id = vValuesNode->id();
  // sread.virtual_table().values_size(); multi rows
  int64_t numRows = vValuesNode->values().size();
  // should be the same value.kFieldsFieldNumber  = vOutputType->size();
  int64_t numColumns;
  // multi rows, each row is a RowVectorPrt

  for (int64_t row = 0; row < numRows; ++row) {
    // the specfic row
    io::substrait::Expression_Literal_Struct* sLitValue =
        sVirtualTable->add_values();
    RowVectorPtr rowValue = vValuesNode->values().at(row);
    // the column numbers in the specfic row.
    numColumns = rowValue->childrenSize();

    for (int64_t column = 0; column < numColumns; ++column) {
      io::substrait::Expression_Literal* sField;

      VectorPtr children = rowValue->childAt(column);
      sField =
          v2STypeConvertor.processVeloxValueByType(sLitValue, sField, children);
    }
  }
}

void VeloxToSubstraitPlanConvertor::transformVAggregateNode(
    std::shared_ptr<const AggregationNode> vAggNode,
    io::substrait::AggregateRel* sAggRel,
    io::substrait::Type_NamedStruct* sGlobalMapping) {
  PlanNodeId vPlanNodeId = vAggNode->id();
  AggregationNode::Step vStep = vAggNode->step();
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> vGroupingKeys =
      vAggNode->groupingKeys();
  std::vector<std::string> vAggregateNames = vAggNode->aggregateNames();
  std::vector<std::shared_ptr<const CallTypedExpr>> vAggregates =
      vAggNode->aggregates();
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> vAggregateMasks =
      vAggNode->aggregateMasks();
  // TODO now this value must be false
  bool vIgnoreNullKeys = vAggNode->ignoreNullKeys();
  std::shared_ptr<const PlanNode> vSource = vAggNode->sources()[0];
  const RowTypePtr vOutput = vAggNode->outputType();

  io::substrait::Rel* sAggInput = sAggRel->mutable_input();
  veloxToSubstraitIR(vSource, sAggInput);

  io::substrait::RelCommon_Emit* sAggEmit =
      sAggRel->mutable_common()->mutable_emit();
  io::substrait::Type_NamedStruct* sNewOutMapping =
      sAggEmit->add_output_mapping();
  io::substrait::Type* sGlobalMappingStructType =
      sGlobalMapping->mutable_struct_()->add_types();

  // set the value of substrait agg emit.
  int64_t vOutputSize = vOutput->size();
  int64_t vOutputChildSize = vOutput->children().size();
  int64_t VoutputNameSize = vOutput->names().size();
  VELOX_CHECK_EQ(
      vOutputSize,
      vOutputChildSize,
      "check the number of Velox Output and it's children size");
  VELOX_CHECK_EQ(
      VoutputNameSize,
      vOutputChildSize,
      "check the number of Velox Output Names and Velox Output children size");

  for (int i = 0; i < vOutputSize; i++) {
    sNewOutMapping->add_index(i);
    auto vOutputName = vOutput->names().at(i);
    sNewOutMapping->add_names(vOutputName);
    auto vOutputchildType = vOutput->children().at(i);
    io::substrait::Type* sOutMappingStructType =
        sNewOutMapping->mutable_struct_()->add_types();
    v2STypeConvertor.veloxTypeToSubstrait(
        vOutputchildType, sOutMappingStructType);
  }

  // TODO need to add the processing of the situation with GROUPING SETS
  // or need to check what vGroupingKeys will be when there have GROUPING SETS
  io::substrait::AggregateRel_Grouping* sAggGroupings =
      sAggRel->add_groupings();
  int64_t vGroupingKeysSize = vGroupingKeys.size();
  for (int64_t i = 0; i < vGroupingKeysSize; i++) {
    std::shared_ptr<const FieldAccessTypedExpr> vGroupingKey =
        vGroupingKeys.at(i);
    io::substrait::Expression* sAggGroupingExpr =
        sAggGroupings->add_grouping_expressions();
    v2SExprConvertor.transformVExpr(
        sAggGroupingExpr, vGroupingKey, sGlobalMapping);
  }

  // vAggregatesSize should be equal or greter than the vAggregateMasks Size
  //  two cases: 1. vAggregateMasksSize = 0, vAggregatesSize>
  //  vAggregateMasksSize
  //  2. vAggregateMasksSize != 0, vAggregatesSize = vAggregateMasksSize
  int64_t vAggregatesSize = vAggregates.size();
  int64_t vAggregateMasksSize = vAggregateMasks.size();

  for (int64_t i = 0; i < vAggregateMasksSize; i++) {
    std::shared_ptr<const FieldAccessTypedExpr> vAggMaskExpr =
        vAggregateMasks.at(i);
    // to see what this will be like linenume_7_true>
    if (vAggMaskExpr.get()) {
      std::string vAggMaskName = vAggMaskExpr->name();
      std::shared_ptr<const Type> vAggMaskType = vAggMaskExpr->type();
      int64_t sGlobalMappingSize = sGlobalMapping->index_size();
      sGlobalMapping->add_index(sGlobalMappingSize + 1);
      sGlobalMapping->add_names(vAggMaskName);
      v2STypeConvertor.veloxTypeToSubstrait(
          vAggMaskType, sGlobalMappingStructType);
    }
  }

  for (int64_t i = 0; i < vAggregatesSize; i++) {
    io::substrait::AggregateRel_Measure* sAggMeasures = sAggRel->add_measures();
    std::shared_ptr<const CallTypedExpr> vAggregatesExpr = vAggregates.at(i);
    io::substrait::Expression_AggregateFunction* sAggFunction =
        sAggMeasures->mutable_measure();

    io::substrait::Expression* sAggFunctionExpr = sAggFunction->add_args();
    v2SExprConvertor.transformVExpr(
        sAggFunctionExpr, vAggregatesExpr, sGlobalMapping);

    std::string vFunName = vAggregatesExpr->name();
    int64_t sFunId = v2SFuncConvertor.registerSFunction(vFunName);
    sAggFunction->mutable_id()->set_id(sFunId);

    std::shared_ptr<const Type> vFunOutputType = vAggregatesExpr->type();
    io::substrait::Type* sAggFunOutputType =
        sAggFunction->mutable_output_type();
    v2STypeConvertor.veloxTypeToSubstrait(vFunOutputType, sAggFunOutputType);

    switch (vStep) {
      case core::AggregationNode::Step::kPartial: {
        sAggFunction->set_phase(
            io::substrait::
                Expression_AggregationPhase_AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE);
        break;
      }
      case core::AggregationNode::Step::kIntermediate: {
        sAggFunction->set_phase(
            io::substrait::
                Expression_AggregationPhase_AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE);
        break;
      }
      case core::AggregationNode::Step::kSingle: {
        sAggFunction->set_phase(
            io::substrait::
                Expression_AggregationPhase_AGGREGATION_PHASE_INITIAL_TO_RESULT);
        break;
      }
      case core::AggregationNode::Step::kFinal: {
        sAggFunction->set_phase(
            io::substrait::
                Expression_AggregationPhase_AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);
        break;
      }
      default:
        std::runtime_error(
            "Unsupport Aggregate Step " + mapAggregationStepToName(vStep) +
            "in Substrait");
    }

    // add new column(the result of the aggregate) to the sGlobalMapping
    int64_t sGlobalMappingSize = sGlobalMapping->index_size();
    sGlobalMapping->add_index(sGlobalMappingSize + 1);
    sGlobalMapping->add_names(vAggregatesExpr->toString());
    v2STypeConvertor.veloxTypeToSubstrait(
        vFunOutputType, sGlobalMappingStructType);

    //  TODO need to verify
    //  transform the mask Expr if have.
    if (vAggregateMasksSize != 0) {
      io::substrait::Expression* sAggFilter = sAggMeasures->mutable_filter();
      // TODO what will happened if the expr is ""?
      std::shared_ptr<const FieldAccessTypedExpr> vAggregateMask =
          vAggregateMasks.at(i);
      if (vAggregateMask.get()) {
        v2SExprConvertor.transformVExpr(
            sAggFilter, vAggregateMask, sGlobalMapping);
      }
    }
  }
}

void VeloxToSubstraitPlanConvertor::transformVProjNode(
    std::shared_ptr<const ProjectNode> vProjNode,
    io::substrait::ProjectRel* sProjRel,
    io::substrait::Type_NamedStruct* sGlobalMapping) {
  const PlanNodeId vId = vProjNode->id();
  std::vector<std::string> vNames = vProjNode->names();
  std::vector<std::shared_ptr<const ITypedExpr>> vProjections =
      vProjNode->projections();
  std::shared_ptr<const PlanNode> vSource = vProjNode->sources()[0];

  const RowTypePtr vOutput = vProjNode->outputType();

  io::substrait::Rel* sProjInput = sProjRel->mutable_input();
  veloxToSubstraitIR(vSource, sProjInput);

  io::substrait::RelCommon_Emit* sProjEmit =
      sProjRel->mutable_common()->mutable_emit();
  io::substrait::Type_NamedStruct* sNewOutMapping =
      sProjEmit->add_output_mapping();
  io::substrait::Type* sGlobalMappingStructType =
      sGlobalMapping->mutable_struct_()->add_types();

  int64_t vProjectionSize = vProjections.size();
  for (int64_t i = 0; i < vProjectionSize; i++) {
    std::shared_ptr<const ITypedExpr>& vExpr = vProjections.at(i);
    io::substrait::Expression* sExpr = sProjRel->add_expressions();

    v2SExprConvertor.transformVExpr(sExpr, vExpr, sGlobalMapping);
    // add outputMapping for each vExpr
    const std::shared_ptr<const Type> vExprType = vExpr->type();
    io::substrait::Type* sOutMappingStructType =
        sNewOutMapping->mutable_struct_()->add_types();
    v2STypeConvertor.veloxTypeToSubstrait(vExprType, sOutMappingStructType);

    sNewOutMapping->add_index(i);
    sNewOutMapping->add_names(vNames[i]);
    // TODO: or just use this :sNewOutMapping->add_names(vNames[i]);
    if (auto vFieldExpr =
            std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vExpr)) {
      std::string vExprName = vFieldExpr->name();
      // sNewOutMapping->add_names(vExprName);
      continue;
    } else if (
        auto vCallTypeExpr =
            std::dynamic_pointer_cast<const CallTypedExpr>(vExpr)) {
      // sNewOutMapping->add_names(vCallTypeExpr->toString());
      // TODO alias names should be add here?

      // add here  globalMapping
      auto sGlobalSize = sGlobalMapping->index_size();
      sGlobalMapping->add_index(sGlobalSize + 1);
      sGlobalMapping->add_names(vCallTypeExpr->toString());
      v2STypeConvertor.veloxTypeToSubstrait(
          vExprType, sGlobalMappingStructType);

    } else {
      LOG(WARNING) << "the type haven't added" << std::endl;
    }
  }

  return;
}

void VeloxToSubstraitPlanConvertor::transformVOrderBy(
    std::shared_ptr<const OrderByNode> vOrderbyNode,
    io::substrait::SortRel* sSortRel) {
  // TODO
}

void VeloxToSubstraitPlanConvertor::transformVPartitionedOutputNode(
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
    v2STypeConvertor.vRowTypePtrToSNamedStruct(vOutPut, sOutputMapping);
  }

  //  Back to handle source node
  veloxToSubstraitIR(
      vPartitionedOutputNode->sources()[0], sDistRel->mutable_input());

  //  TODO miss  the parameter  bool replicateNullsAndAny
}

void VeloxToSubstraitPlanConvertor::transformVPartitionFunc(
    io::substrait::DistributeRel* sDistRel,
    std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode) {
  std::shared_ptr<PartitionFunction> factory =
      vPartitionedOutputNode->partitionFunctionFactory()(
          vPartitionedOutputNode->numPartitions());
  if (auto f = std::dynamic_pointer_cast<velox::exec::HashPartitionFunction>(
          factory)) {
    auto func_id = v2SFuncConvertor.registerSFunction("HashPartitionFunction");
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
    auto func_id =
        v2SFuncConvertor.registerSFunction("RoundRobinPartitionFunction");
    auto scala_function =
        sDistRel->mutable_d_field()->mutable_expr()->mutable_scalar_function();
    scala_function->mutable_id()->set_id(func_id);
    // TODO add keys

  } else if (
      auto f = std::dynamic_pointer_cast<
          velox::connector::hive::HivePartitionFunction>(factory)) {
    auto func_id = v2SFuncConvertor.registerSFunction("HivePartitionFunction");
    auto scala_function =
        sDistRel->mutable_d_field()->mutable_expr()->mutable_scalar_function();
    scala_function->mutable_id()->set_id(func_id);
    // TODO add keys
  }
}

// Substrait to Velox Plan
std::shared_ptr<const PlanNode>
SubstraitToVeloxPlanConvertor::substraitIRToVelox(
    const io::substrait::Plan& sPlan) {
  s2VFuncConvertor.initFunctionMap();
  const io::substrait::Rel& sRel = sPlan.relations(0);
  return substraitIRToVelox(sRel, 0);
}

std::shared_ptr<const PlanNode>
SubstraitToVeloxPlanConvertor::substraitIRToVelox(
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

std::shared_ptr<FilterNode> SubstraitToVeloxPlanConvertor::transformSFilter(
    const io::substrait::Rel& sRel,
    int depth) {
  const io::substrait::FilterRel& sFilter = sRel.filter();
  std::shared_ptr<const PlanNode> vSource =
      substraitIRToVelox(sFilter.input(), depth + 1);

  if (!sFilter.has_condition()) {
    return std::make_shared<FilterNode>(
        std::to_string(depth), nullptr, vSource);
  }

  const io::substrait::Expression& sExpr = sFilter.condition();
  return std::make_shared<FilterNode>(
      std::to_string(depth),
      s2VExprConvertor.transformSExpr(sExpr, sGlobalMapping_),
      vSource);
}

std::shared_ptr<PlanNode> SubstraitToVeloxPlanConvertor::transformSRead(
    const io::substrait::Rel& sRel,
    int depth) {
  const io::substrait::ReadRel& sRead = sRel.read();
  std::shared_ptr<const velox::RowType> vOutputType =
      s2VTypeConvertor.sNamedStructToVRowTypePtr(sRead.base_schema());

  // TODO need to add the impl of type local_files

  if (sRead.has_filter()) {
    return std::make_shared<FilterNode>(
        std::to_string(depth),
        s2VExprConvertor.transformSExpr(sRead.filter(), sGlobalMapping_),
        substraitIRToVelox(sRel, depth + 1));
  }

  if (sRead.has_projection()) {
    throw std::runtime_error("Unsupported projection in sRead ");
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
    pool_ = scopedPool.get();

    // TODO this should be the vector.size* batchSize .
    int64_t numRows = sRead.virtual_table().values_size();
    int64_t numColumns = vOutputType->size();
    int64_t valueFieldNums =
        sRead.virtual_table().values(numRows - 1).fields_size();

    std::vector<RowVectorPtr> vectors;
    bool nullFlag = false;

    int64_t batchSize = valueFieldNums / numColumns;

    for (int32_t row = 0; row < numRows; ++row) {
      std::vector<VectorPtr> children;
      std::shared_ptr<RowVector> rowVector;
      io::substrait::Expression_Literal_Struct sRowValue =
          sRead.virtual_table().values(row);
      int64_t sFieldSize = sRowValue.fields_size();
      int64_t vChildrenSize = vOutputType->children().size();
      for (int col = 0; col < vChildrenSize; col++) {
        io::substrait::Expression_Literal sField =
            sRowValue.fields(col * batchSize);
        io::substrait::Expression_Literal::LiteralTypeCase sFieldType =
            sField.literal_type_case();
        std::shared_ptr<const Type> vOutputChildType =
            vOutputType->childAt(col);
        VectorPtr childrenValue;
        // for the null value
        if (sFieldType == 29) {
          nullFlag = true;
          childrenValue = BaseVector::createNullConstant(
              vOutputChildType, batchSize, pool_);
        } else {
          childrenValue = VELOX_DYNAMIC_TYPE_DISPATCH(
              test::BatchMaker::createVector,
              vOutputChildType->kind(),
              vOutputType->childAt(col),
              batchSize,
              *scopedPool);
        }
        children.emplace_back(childrenValue);
      }
      if (nullFlag) {
        rowVector = std::make_shared<RowVector>(
            pool_, vOutputType, BufferPtr(nullptr), batchSize, children);

      } else {
        rowVector = std::make_shared<RowVector>(
            pool_, vOutputType, BufferPtr(), batchSize, children);
      }
      vectors.emplace_back(rowVector);
    }

    return std::make_shared<ValuesNode>(
        std::to_string(depth), move(vectors), parallelizable);
  }
}

std::shared_ptr<AggregationNode>
SubstraitToVeloxPlanConvertor::transformSAggregate(
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

  const io::substrait::AggregateRel& sAgg = sRel.aggregate();
  std::shared_ptr<const PlanNode> vSource =
      substraitIRToVelox(sAgg.input(), depth + 1);

  // TODO need to confirm whether this is only for one grouping set, GROUP BY
  // a,b,c. Not fit for GROUPING SETS ???
  for (auto& sGroup : sAgg.groupings()) {
    for (auto& sExpr : sGroup.grouping_expressions()) {
      std::shared_ptr<const ITypedExpr> vGroupingKey =
          s2VExprConvertor.transformSExpr(sExpr, sGlobalMapping_);
      groupingKey =
          std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vGroupingKey);
      groupingKeys.push_back(groupingKey);
    }
  }
  // for velox  sum(c) is ok, but sum(c + d) is not.
  for (auto& sMeas : sAgg.measures()) {
    io::substrait::Expression_AggregateFunction sMeasure = sMeas.measure();
    if (sMeas.has_filter()) {
      io::substrait::Expression sAggMask = sMeas.filter();
      // handle the case sum(IF(linenumber = 7, partkey)) <=>sum(partkey) FILTER
      // (where linenumber = 7) For each measure, an optional boolean input
      // column that is used to mask out rows for this particular measure.
      size_t sAggMaskLength = sAggMask.ByteSizeLong();
      if (sAggMaskLength == 0) {
        aggregateMask = {};
      } else {
        std::shared_ptr<const ITypedExpr> vAggMask =
            s2VExprConvertor.transformSExpr(sAggMask, sGlobalMapping_);
        aggregateMask =
            std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vAggMask);
      }
      aggregateMasks.push_back(aggregateMask);
    }

    std::vector<std::shared_ptr<const ITypedExpr>> children;
    std::string out_name;
    std::string function_name =
        s2VFuncConvertor.FindFunction(sMeasure.id().id());
    out_name = function_name;
    // AggregateFunction.args should be one for velox . if not, should do
    // project firstly
    int64_t sMeasureArgSize = sMeasure.args_size();
    // the very simple case for sum(a) need to check if this will contain the
    // situation with maskExpression.
    if (sMeasureArgSize == 1) {
      auto vMeasureArgExpr =
          s2VExprConvertor.transformSExpr(sMeasure.args()[0], sGlobalMapping_);
      if (auto vMeasureArg =
              std::dynamic_pointer_cast<const CallTypedExpr>(vMeasureArgExpr)) {
        aggregates.push_back(vMeasureArg);
        out_name += vMeasureArg->toString();
        aggregateNames.push_back(out_name);
      }
    } else { // the case for sum(a+b)
      // TODO do project firstly
      //  get the result of c+d then do agg
    }

    switch (sMeas.measure().phase()) {
      case io::substrait::Expression_AggregationPhase::
          Expression_AggregationPhase_AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE: {
        step = AggregationNode::Step::kPartial;
        break;
      }
      case io::substrait::Expression_AggregationPhase::
          Expression_AggregationPhase_AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT: {
        step = AggregationNode::Step::kFinal;
        break;
      }
      case io::substrait::Expression_AggregationPhase::
          Expression_AggregationPhase_AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE: {
        step = AggregationNode::Step::kIntermediate;
        break;
      }
      case io::substrait::Expression_AggregationPhase::
          Expression_AggregationPhase_AGGREGATION_PHASE_INITIAL_TO_RESULT: {
        step = AggregationNode::Step::kSingle;
        break;
      }
      default:
        VELOX_UNSUPPORTED("Unsupported aggregation step");
    }
  }

  return std::make_shared<AggregationNode>(
      std::to_string(depth),
      step,
      groupingKeys,
      aggregateNames,
      aggregates,
      aggregateMasks,
      ignoreNullKeys,
      vSource);
}
std::shared_ptr<ProjectNode> SubstraitToVeloxPlanConvertor::transformSProject(
    const io::substrait::Rel& sRel,
    int depth) {
  const io::substrait::ProjectRel& sProj = sRel.project();
  std::vector<std::shared_ptr<const ITypedExpr>> vExpressions;
  std::vector<std::string> names;

  std::shared_ptr<const PlanNode> vSource =
      substraitIRToVelox(sProj.input(), depth + 1);

  for (auto& sExpr : sProj.expressions()) {
    std::shared_ptr<const ITypedExpr> vExpr =
        s2VExprConvertor.transformSExpr(sExpr, sGlobalMapping_);
    vExpressions.push_back(vExpr);
  }
  // TODO check if there should be depth? now it's only one output_mapping, so
  // depth = 0 is right for the simple case(proj->values)
  // sProjOutMap = sProj.common().emit().output_mapping(depth);
  io::substrait::Type_NamedStruct sProjOutMap =
      sProj.common().emit().output_mapping(0);
  // the proj common is always start from 0. because the way we trans from velox
  // to substrait.
  int64_t sProjOutMapSize = sProjOutMap.index_size();
  for (int64_t i = 0; i < sProjOutMapSize; i++) {
    names.push_back(sProjOutMap.names(i));
  }

  std::shared_ptr<ProjectNode> vProjNode = std::make_shared<ProjectNode>(
      std::to_string(depth), names, vExpressions, vSource);

  return vProjNode;
}

std::shared_ptr<OrderByNode> SubstraitToVeloxPlanConvertor::transformSSort(
    const io::substrait::Rel& sRel,
    int depth) {
  std::vector<OrderByNode> velox_nodes;
  const io::substrait::SortRel& sSort = sRel.sort();

  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys;
  std::vector<SortOrder> sortingOrders;
  bool isPartial;

  std::shared_ptr<const PlanNode> vSource =
      substraitIRToVelox(sSort.input(), depth + 1);

  isPartial = sSort.common().distribution().d_type() == 0 ? true : false;

  // The supported orders are: ascending nulls first, ascending nulls last,
  // descending nulls first, descending nulls last
  for (const io::substrait::Expression_SortField& sOrderField : sSort.sorts()) {
    // TODO check whether  ssort.common() need to be the node output before
    const io::substrait::Expression sExpr = sOrderField.expr();
    std::shared_ptr<const ITypedExpr> sortingKey =
        s2VExprConvertor.transformSExpr(sExpr, sGlobalMapping_);
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
      vSource);
}

std::shared_ptr<PartitionedOutputNode>
SubstraitToVeloxPlanConvertor::transformSDistribute(
    const io::substrait::Plan& sPlan,
    int depth) {
  // TODO
}

} // namespace facebook::velox