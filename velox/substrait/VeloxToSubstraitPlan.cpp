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

#include "VeloxToSubstraitPlan.h"
#include "GlobalCommonVariable.h"

namespace facebook::velox::substrait {

// Velox Plan to Substrait
void VeloxToSubstraitPlanConvertor::veloxToSubstraitIR(
    std::shared_ptr<const PlanNode> vPlan,
    ::substrait::Plan& sPlan) {
  // Assume only accepts a single plan fragment
  // TODO: convert the Split RootNode get from dispatcher to RootRel
  ::substrait::Rel* sRel = sPlan.add_relations()->mutable_rel();
  veloxToSubstraitIR(vPlan, sRel);
}

void VeloxToSubstraitPlanConvertor::veloxToSubstraitIR(
    std::shared_ptr<const PlanNode> vPlanNode,
    ::substrait::Rel* sRel) {
  // auto nextNode = vPlanNode->sources()[0];
  ::substrait::RelCommon* relCommon;
  if (auto filterNode =
          std::dynamic_pointer_cast<const FilterNode>(vPlanNode)) {
    auto sFilterRel = sRel->mutable_filter();
    transformVFilter(filterNode, sFilterRel);
    relCommon = sFilterRel->mutable_common();
  }
  if (auto aggNode =
          std::dynamic_pointer_cast<const AggregationNode>(vPlanNode)) {
    auto sAggRel = sRel->mutable_aggregate();
    transformVAggregateNode(aggNode, sAggRel);
    relCommon = sAggRel->mutable_common();
  }
  if (auto vValuesNode =
          std::dynamic_pointer_cast<const ValuesNode>(vPlanNode)) {
    ::substrait::ReadRel* sReadRel = sRel->mutable_read();
    transformVValuesNode(vValuesNode, sReadRel);
    relCommon = sReadRel->mutable_common();
  }
  if (auto vProjNode =
          std::dynamic_pointer_cast<const ProjectNode>(vPlanNode)) {
    ::substrait::ProjectRel* sProjRel = sRel->mutable_project();
    transformVProjNode(vProjNode, sProjRel);
    relCommon = sProjRel->mutable_common();
  }
  if (auto partitionedOutputNode =
          std::dynamic_pointer_cast<const PartitionedOutputNode>(vPlanNode)) {
    ::substrait::DistributeRel* dRel = sRel->mutable_distribute();
    dRel->set_partitioncount(partitionedOutputNode->numPartitions());
    transformVPartitionedOutputNode(partitionedOutputNode, dRel);
  }

  // For output node, needs to put distribution info into its source node's
  // relcommon
  // this part can be used to check if partition is enable.
  if (auto partitionedOutputNode =
          std::dynamic_pointer_cast<const PartitionedOutputNode>(vPlanNode)) {
    relCommon->mutable_distribution()->set_d_type(
        ::substrait::RelCommon_Distribution_DISTRIBUTION_TYPE::
            RelCommon_Distribution_DISTRIBUTION_TYPE_PARTITIONED);
  } else {
    relCommon->mutable_distribution()->set_d_type(
        ::substrait::RelCommon_Distribution_DISTRIBUTION_TYPE::
            RelCommon_Distribution_DISTRIBUTION_TYPE_SINGLETON);
  }
  // TODO this is for debug
  LOG(INFO) << "the final " << std::endl;
  sRel->PrintDebugString();
  //    auto d_field = relCommon->mutable_distribution()->mutable_d_field;
}
void VeloxToSubstraitPlanConvertor::transformVFilter(
    std::shared_ptr<const FilterNode> vFilterNode,
    ::substrait::FilterRel* sFilterRel) {
  const PlanNodeId vId = vFilterNode->id();
  std::shared_ptr<const PlanNode> vSource;
  std::vector<std::shared_ptr<const PlanNode>> vSources =
      vFilterNode->sources();
  // check how many inputs there have
  int64_t vSourceSize = vSources.size();
  if (vSourceSize == 0) {
    VELOX_FAIL("Filter Node must have input");
  } else if (vSourceSize == 1) {
    vSource = vSources[0];
  } else {
    // TODO
    // select one in the plan fragment pass to transformVExpr
    //  and the other change into root or simpleCapture.
  }
  std::shared_ptr<const ITypedExpr> vFilterCondition = vFilterNode->filter();

  ::substrait::Rel* sFilterInput = sFilterRel->mutable_input();
  ::substrait::Expression* sFilterCondition = sFilterRel->mutable_condition();
  //   Build source
  veloxToSubstraitIR(vSource, sFilterInput);

  RowTypePtr vPreNodeOutPut = vSource->outputType();
  //   Construct substrait expr
  v2SExprConvertor_.transformVExpr(
      sFilterCondition, vFilterCondition, vPreNodeOutPut);
  sFilterRel->mutable_common()->mutable_direct();
}

void VeloxToSubstraitPlanConvertor::transformVValuesNode(
    std::shared_ptr<const ValuesNode> vValuesNode,
    ::substrait::ReadRel* sReadRel) {
  const RowTypePtr vOutPut = vValuesNode->outputType();

  ::substrait::ReadRel_VirtualTable* sVirtualTable =
      sReadRel->mutable_virtual_table();

  ::substrait::NamedStruct* sBaseSchema = sReadRel->mutable_base_schema();
  v2STypeConvertor_.vRowTypePtrToSNamedStruct(vOutPut, sBaseSchema);

  const PlanNodeId id = vValuesNode->id();
  // sread.virtual_table().values_size(); multi rows
  int64_t numRows = vValuesNode->values().size();
  // should be the same value.kFieldsFieldNumber  = vOutputType->size();
  int64_t numColumns;
  // multi rows, each row is a RowVectorPrt

  for (int64_t row = 0; row < numRows; ++row) {
    // the specfic row
    ::substrait::Expression_Literal_Struct* sLitValue =
        sVirtualTable->add_values();
    RowVectorPtr rowValue = vValuesNode->values().at(row);
    // the column numbers in the specfic row.
    numColumns = rowValue->childrenSize();

    for (int64_t column = 0; column < numColumns; ++column) {
      ::substrait::Expression_Literal* sField;

      VectorPtr children = rowValue->childAt(column);
      sField = v2STypeConvertor_.processVeloxValueByType(
          sLitValue, sField, children);
    }
  }
  sReadRel->mutable_common()->mutable_direct();
}

void VeloxToSubstraitPlanConvertor::transformVAggregateNode(
    std::shared_ptr<const AggregationNode> vAggNode,
    ::substrait::AggregateRel* sAggRel) {
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

  // check how many inputs there have
  std::shared_ptr<const PlanNode> vSource;
  std::vector<std::shared_ptr<const PlanNode>> vSources = vAggNode->sources();

  int64_t vSourceSize = vSources.size();
  if (vSourceSize == 0) {
    VELOX_FAIL("Aggregate Node must have input");
  } else if (vSourceSize == 1) {
    vSource = vSources[0];
  } else {
    // TODO
    // select one in the plan fragment pass to transformVExpr
    //  and the other change into root or simpleCapture.
  }

  const RowTypePtr vOutput = vAggNode->outputType();

  ::substrait::Rel* sAggInput = sAggRel->mutable_input();
  veloxToSubstraitIR(vSource, sAggInput);

  RowTypePtr vPreNodeOutPut = vSource->outputType();
  std::vector<std::string> vPreNodeColNames = vPreNodeOutPut->names();
  std::vector<std::shared_ptr<const velox::Type>> vPreNodeColTypes =
      vPreNodeOutPut->children();
  int64_t vPreNodeColNums = vPreNodeColNames.size();
  int64_t sAggEmitReMapId = vPreNodeColNums;

  ::substrait::RelCommon_Emit* sAggEmit =
      sAggRel->mutable_common()->mutable_emit();
  // TODO
  /*::substrait::NamedStruct* sNewOutMapping =
      sAggEmit->add_output_mapping();*/
  ::substrait::Type* sGlobalMappingStructType =
      sGlobalMapping_->mutable_struct_()->add_types();

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

  // TODO
  /*  for (int i = 0; i < vOutputSize; i++) {
      sNewOutMapping->add_index(i);
      auto vOutputName = vOutput->names().at(i);
      sNewOutMapping->add_names(vOutputName);
      auto vOutputchildType = vOutput->children().at(i);
      ::substrait::Type* sOutMappingStructType =
          sNewOutMapping->mutable_struct_()->add_types();
      v2STypeConvertor.veloxTypeToSubstrait(
          vOutputchildType, sOutMappingStructType);
    }*/

  // TODO need to add the processing of the situation with GROUPING SETS
  // or need to check what vGroupingKeys will be when there have GROUPING SETS
  ::substrait::AggregateRel_Grouping* sAggGroupings = sAggRel->add_groupings();
  int64_t vGroupingKeysSize = vGroupingKeys.size();
  for (int64_t i = 0; i < vGroupingKeysSize; i++) {
    std::shared_ptr<const FieldAccessTypedExpr> vGroupingKey =
        vGroupingKeys.at(i);
    ::substrait::Expression* sAggGroupingExpr =
        sAggGroupings->add_grouping_expressions();
    v2SExprConvertor_.transformVExpr(
        sAggGroupingExpr, vGroupingKey, vPreNodeOutPut);
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
    // TODO
    /*   if (vAggMaskExpr.get()) {
         std::string vAggMaskName = vAggMaskExpr->name();
         std::shared_ptr<const Type> vAggMaskType = vAggMaskExpr->type();
         int64_t sGlobalMappingSize = sGlobalMapping->index_size();
         sGlobalMapping->add_index(sGlobalMappingSize + 1);
         sGlobalMapping->add_names(vAggMaskName);
         v2STypeConvertor.veloxTypeToSubstrait(
             vAggMaskType, sGlobalMappingStructType);
       }*/
  }

  for (int64_t i = 0; i < vAggregatesSize; i++) {
    ::substrait::AggregateRel_Measure* sAggMeasures = sAggRel->add_measures();
    std::shared_ptr<const CallTypedExpr> vAggregatesExpr = vAggregates.at(i);
    ::substrait::AggregateFunction* sAggFunction =
        sAggMeasures->mutable_measure();

    ::substrait::Expression* sAggFunctionExpr = sAggFunction->add_args();
    v2SExprConvertor_.transformVExpr(
        sAggFunctionExpr, vAggregatesExpr, vPreNodeOutPut);

    std::string vFunName = vAggregatesExpr->name();
    int64_t sFunId = v2SFuncConvertor_.registerSFunction(vFunName);
    sAggFunction->set_function_reference(sFunId);

    std::shared_ptr<const Type> vFunOutputType = vAggregatesExpr->type();
    ::substrait::Type* sAggFunOutputType = sAggFunction->mutable_output_type();
    v2STypeConvertor_.veloxTypeToSubstrait(vFunOutputType, sAggFunOutputType);

    switch (vStep) {
      case core::AggregationNode::Step::kPartial: {
        sAggFunction->set_phase(
            ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE);
        break;
      }
      case core::AggregationNode::Step::kIntermediate: {
        sAggFunction->set_phase(
            ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE);
        break;
      }
      case core::AggregationNode::Step::kSingle: {
        sAggFunction->set_phase(
            ::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
        break;
      }
      case core::AggregationNode::Step::kFinal: {
        sAggFunction->set_phase(
            ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);
        break;
      }
      default:
        std::runtime_error(
            "Unsupport Aggregate Step " + mapAggregationStepToName(vStep) +
            "in Substrait");
    }

    // add new column(the result of the aggregate) to the sGlobalMapping
    // TODO
    /*    int64_t sGlobalMappingSize = sGlobalMapping->index_size();
        sGlobalMapping->add_index(sGlobalMappingSize + 1);
        sGlobalMapping->add_names(vAggregatesExpr->toString());
        v2STypeConvertor.veloxTypeToSubstrait(
            vFunOutputType, sGlobalMappingStructType);*/

    //  TODO need to verify
    //  transform the mask Expr if have.
    if (vAggregateMasksSize != 0) {
      ::substrait::Expression* sAggFilter = sAggMeasures->mutable_filter();
      // TODO what will happened if the expr is ""?
      std::shared_ptr<const FieldAccessTypedExpr> vAggregateMask =
          vAggregateMasks.at(i);
      if (vAggregateMask.get()) {
        v2SExprConvertor_.transformVExpr(
            sAggFilter, vAggregateMask, vPreNodeOutPut);
      }
    }
  }
}

void VeloxToSubstraitPlanConvertor::transformVProjNode(
    std::shared_ptr<const ProjectNode> vProjNode,
    ::substrait::ProjectRel* sProjRel) {
  // the info from vProjNode
  const PlanNodeId vId = vProjNode->id();
  std::vector<std::string> vNames = vProjNode->names();
  std::vector<std::shared_ptr<const ITypedExpr>> vProjections =
      vProjNode->projections();
  const RowTypePtr vOutput = vProjNode->outputType();

  // check how many inputs there have
  std::vector<std::shared_ptr<const PlanNode>> vSources = vProjNode->sources();
  // the PreNode
  std::shared_ptr<const PlanNode> vSource;
  int64_t vSourceSize = vSources.size();
  if (vSourceSize == 0) {
    VELOX_FAIL("Project Node must have input");
  } else if (vSourceSize == 1) {
    vSource = vSources[0];
  } else {
    // TODO
    // select one in the plan fragment pass to transformVExpr
    //  and the other change into root or simpleCapture.
  }

  // process the source Node.
  ::substrait::Rel* sProjInput = sProjRel->mutable_input();
  veloxToSubstraitIR(vSource, sProjInput);

  // remapping the output
  ::substrait::RelCommon_Emit* sProjEmit =
      sProjRel->mutable_common()->mutable_emit();

  int64_t vProjectionSize = vProjections.size();

  RowTypePtr vPreNodeOutPut = vSource->outputType();
  std::vector<std::string> vPreNodeColNames = vPreNodeOutPut->names();
  std::vector<std::shared_ptr<const velox::Type>> vPreNodeColTypes =
      vPreNodeOutPut->children();
  int64_t vPreNodeColNums = vPreNodeColNames.size();
  int64_t sProjEmitReMapId = vPreNodeColNums;

  for (int64_t i = 0; i < vProjectionSize; i++) {
    std::shared_ptr<const ITypedExpr>& vExpr = vProjections.at(i);
    ::substrait::Expression* sExpr = sProjRel->add_expressions();

    v2SExprConvertor_.transformVExpr(sExpr, vExpr, vPreNodeOutPut);
    // add outputMapping for each vExpr
    const std::shared_ptr<const Type> vExprType = vExpr->type();

    bool sProjEmitReMap = false;
    for (int64_t j = 0; j < vPreNodeColNums; j++) {
      if (vExprType == vPreNodeColTypes[j] &&
          vOutput->nameOf(i) == vPreNodeColNames[j]) {
        sProjEmit->add_output_mapping(j);
        sProjEmitReMap = true;
        break;
      }
    }
    if (!sProjEmitReMap) {
      sProjEmit->add_output_mapping(sProjEmitReMapId++);
    }
  }

  return;
}

void VeloxToSubstraitPlanConvertor::transformVOrderBy(
    std::shared_ptr<const OrderByNode> vOrderbyNode,
    ::substrait::SortRel* sSortRel) {
  // TODO
}

void VeloxToSubstraitPlanConvertor::transformVPartitionedOutputNode(
    std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode,
    ::substrait::DistributeRel* sDistRel) {
  if (vPartitionedOutputNode->isBroadcast()) {
    sDistRel->set_type(::substrait::DistributeRel_DistributeType::
                           DistributeRel_DistributeType_boradcast);
  } else {
    sDistRel->set_type(::substrait::DistributeRel_DistributeType::
                           DistributeRel_DistributeType_scatter);
  }

  // Transform distribution function
  transformVPartitionFunc(sDistRel, vPartitionedOutputNode);

  // Handle emit for output
  std::vector<std::shared_ptr<const ::substrait::Type>> sTypes;
  const RowTypePtr vOutPut = vPartitionedOutputNode->outputType();
  std::vector<std::string> names = vOutPut->names();
  std::vector<std::shared_ptr<const velox::Type>> vTypes = vOutPut->children();

  int64_t vOutSize = vOutPut->size();
  ::substrait::RelCommon_Emit* sOutputEmit =
      sDistRel->mutable_common()->mutable_emit();

  // TODO
  /*
    for (int64_t i = 0; i < vOutSize; i++) {
      ::substrait::NamedStruct* sOutputMapping =
          sOutputEmit->mutable_output_mapping(i);
      v2STypeConvertor.vRowTypePtrToSNamedStruct(vOutPut, sOutputMapping);
    }
  */

  //  Back to handle source node
  veloxToSubstraitIR(
      vPartitionedOutputNode->sources()[0], sDistRel->mutable_input());

  //  TODO miss  the parameter  bool replicateNullsAndAny
}

void VeloxToSubstraitPlanConvertor::transformVPartitionFunc(
    ::substrait::DistributeRel* sDistRel,
    std::shared_ptr<const PartitionedOutputNode> vPartitionedOutputNode) {
  std::shared_ptr<PartitionFunction> factory =
      vPartitionedOutputNode->partitionFunctionFactory()(
          vPartitionedOutputNode->numPartitions());
  if (auto f = std::dynamic_pointer_cast<velox::exec::HashPartitionFunction>(
          factory)) {
    auto func_id = v2SFuncConvertor_.registerSFunction("HashPartitionFunction");
    auto scala_function =
        sDistRel->mutable_d_field()->mutable_expr()->mutable_scalar_function();
    scala_function->set_function_reference(func_id);
    // TODO: add parameters
    //  //velox std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
    //  to substrait selection FieldReference
    auto keys = vPartitionedOutputNode->keys();
    // TODO  TransformVExpr(velox::Expression &vexpr, ::substrait::Expression
    // &sexpr)
    //  velox FieldAccessTypedExpr to substrait selection.
    auto outputInfo = sDistRel->common().emit().output_mapping();
    // scala_function->add_args()->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(partitionedOutputNode->keys());

  } else if (
      auto f =
          std::dynamic_pointer_cast<velox::exec::RoundRobinPartitionFunction>(
              factory)) {
    auto func_id =
        v2SFuncConvertor_.registerSFunction("RoundRobinPartitionFunction");
    auto scala_function =
        sDistRel->mutable_d_field()->mutable_expr()->mutable_scalar_function();
    scala_function->set_function_reference(func_id);
    // TODO add keys

  } else if (
      auto f = std::dynamic_pointer_cast<
          velox::connector::hive::HivePartitionFunction>(factory)) {
    auto func_id = v2SFuncConvertor_.registerSFunction("HivePartitionFunction");
    auto scala_function =
        sDistRel->mutable_d_field()->mutable_expr()->mutable_scalar_function();
    scala_function->set_function_reference(func_id);
    // TODO add keys
  }
}
} // namespace facebook::velox::substrait
