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

#include "velox/dwio/dwrf/test/utils/BatchMaker.h"

#include "SubstraitIRConverter.h"

namespace facebook::velox {

// Consume substrait rel
// Public API:
std::shared_ptr<const PlanNode> SubstraitVeloxConvertor::fromSubstraitIR(
    const io::substrait::Plan& sPlan) {
  initFunctionMap();
  return fromSubstraitIR(sPlan, 0);
}

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
  const io::substrait::Rel& sRel = sPlan.relations(depth);
  return fromSubstraitIR(sRel, depth);
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

void SubstraitVeloxConvertor::initFunctionMap() {
  for (auto& sMap : plan.mappings()) {
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
        "Could not find function with id: " + std::to_string(id));
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
  std::shared_ptr<const PlanNode> vSource =
      fromSubstraitIR(sFilter.input(), depth + 1);

  if (!sFilter.has_condition()) {
    return std::make_shared<FilterNode>(
        std::to_string(depth), nullptr, vSource);
  }

  const io::substrait::Expression& sExpr = sFilter.condition();
  return std::make_shared<FilterNode>(
      std::to_string(depth), transformSExpr(sExpr, sGlobalMapping), vSource);
}

std::shared_ptr<const ITypedExpr>
SubstraitVeloxConvertor::transformSLiteralExpr(
    const io::substrait::Expression_Literal& sLiteralExpr) {
  variant sLiteralExprVariant = transformSLiteralType(sLiteralExpr);
  return std::make_shared<ConstantTypedExpr>(sLiteralExprVariant);
}

variant SubstraitVeloxConvertor::transformSLiteralType(
    const io::substrait::Expression_Literal& sLiteralExpr) {
  switch (sLiteralExpr.literal_type_case()) {
    case io::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
      // Mapping the kDecimal in Substrait to DOUBLE in Velox
      return velox::variant(sLiteralExpr.fp64());
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kString: {
      return velox::variant(sLiteralExpr.var_char());
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kVarChar: {
      return velox::variant(sLiteralExpr.var_char());
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kFixedChar: {
      return velox::variant(sLiteralExpr.var_char());
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
      return velox::variant(sLiteralExpr.boolean());
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kI64: {
      return velox::variant(sLiteralExpr.i64());
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kI32: {
      return velox::variant(sLiteralExpr.i32());
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kI16: {
      return velox::variant(static_cast<int16_t>(sLiteralExpr.i16()));
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kI8: {
      return velox::variant(static_cast<int8_t>(sLiteralExpr.i8()));
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kFp64: {
      return velox::variant(sLiteralExpr.fp64());
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kFp32: {
      return velox::variant(sLiteralExpr.fp32());
    }
    case io::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      io::substrait::Type nullValue = sLiteralExpr.null();
      return processSubstraitLiteralNullType(sLiteralExpr, nullValue);
    }
    default:
      throw std::runtime_error(
          "Unsupported liyeral_type in transformSLiteralType " +
          std::to_string(sLiteralExpr.literal_type_case()));
  }
}

variant SubstraitVeloxConvertor::processSubstraitLiteralNullType(
    const io::substrait::Expression_Literal& sLiteralExpr,
    io::substrait::Type nullType) {
  switch (nullType.kind_case()) {
    case io::substrait::Type::kDecimal: {
      // mapping to DOUBLE
      return velox::variant(sLiteralExpr.fp64());
    }
    case io::substrait::Type::kString: {
      return velox::variant(sLiteralExpr.var_char());
    }
    case io::substrait::Type::kBool: {
      return velox::variant(sLiteralExpr.boolean());
    }
    case io::substrait::Type::kI64: {
      return velox::variant(sLiteralExpr.i64());
    }
    case io::substrait::Type::kI32: {
      return velox::variant(sLiteralExpr.i32());
    }
    case io::substrait::Type::kI16: {
      return velox::variant(static_cast<int16_t>(sLiteralExpr.i16()));
    }
    case io::substrait::Type::kI8: {
      return velox::variant(static_cast<int8_t>(sLiteralExpr.i8()));
    }
    case io::substrait::Type::kFp64: {
      return velox::variant(sLiteralExpr.fp64());
    }
    case io::substrait::Type::kFp32: {
      return velox::variant(sLiteralExpr.fp32());
    }
    default:
      throw std::runtime_error(
          "Unsupported type in processSubstraitLiteralNullType " +
          std::to_string(nullType.kind_case()));
  }
}

std::shared_ptr<const ITypedExpr> SubstraitVeloxConvertor::transformSExpr(
    const io::substrait::Expression& sExpr,
    io::substrait::Type_NamedStruct* sGlobalMapping) {
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

      auto outId = sExpr.selection().direct_reference().struct_field().field();
      int64_t sGlobalMapSize = sGlobalMapping->index_size();
      for (int64_t i = 0; i < sGlobalMapSize; i++) {
        if (sGlobalMapping->index(i) == outId) {
          auto sName = sGlobalMapping->names(i);
          auto sType = sGlobalMapping->mutable_struct_()->types(i);
          velox::TypePtr vType = substraitTypeToVelox(sType);
          // convert type to row
          return std::make_shared<FieldAccessTypedExpr>(
              vType, std::make_shared<InputTypedExpr>(vType), sName);
        }
      }
    }
    case io::substrait::Expression::RexTypeCase::kScalarFunction: {
      io::substrait::Expression_ScalarFunction sScalarFunc =
          sExpr.scalar_function();
      io::substrait::Type sScalaFunOutType = sScalarFunc.output_type();
      velox::TypePtr vScalaFunType = substraitTypeToVelox(sScalaFunOutType);

      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sGlobalMapping));
      }
      // TODO search function name by yaml extension
      std::string function_name =
          FindFunction(sExpr.scalar_function().id().id());
      //  and or  try concatrow
      if (function_name != "if" && function_name != "switch") {
        return std::make_shared<CallTypedExpr>(
            vScalaFunType, children, function_name);
      }
    }
    case io::substrait::Expression::RexTypeCase::kIfThen: {
      io::substrait::Expression_ScalarFunction sScalarFunc =
          sExpr.scalar_function();
      io::substrait::Type sScalaFunOutType = sScalarFunc.output_type();
      velox::TypePtr vScalaFunType = substraitTypeToVelox(sScalaFunOutType);

      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sGlobalMapping));
      }
      return std::make_shared<velox::core::CallTypedExpr>(
          vScalaFunType, move(children), "if");
    }
    case io::substrait::Expression::RexTypeCase::kSwitchExpression: {
      io::substrait::Expression_ScalarFunction sScalarFunc =
          sExpr.scalar_function();
      io::substrait::Type sScalaFunOutType = sScalarFunc.output_type();
      velox::TypePtr vScalaFunType = substraitTypeToVelox(sScalaFunOutType);
      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sGlobalMapping));
      }
      return std::make_shared<velox::core::CallTypedExpr>(
          vScalaFunType, move(children), "switch");
    }
    case io::substrait::Expression::kCast: {
      io::substrait::Expression_Cast sCastExpr = sExpr.cast();

      io::substrait::Type sCastType = sCastExpr.type();
      std::shared_ptr<const Type> vCastType = substraitTypeToVelox(sCastType);

      // TODO add flag in substrait after. now is set false.
      bool nullOnFailure = false;

      std::vector<std::shared_ptr<const ITypedExpr>> vCastInputs;
      io::substrait::Expression sCastInput = sCastExpr.input();
      std::shared_ptr<const ITypedExpr> vCastInput =
          transformSExpr(sCastInput, sGlobalMapping);
      vCastInputs.emplace_back(vCastInput);

      return std::make_shared<CastTypedExpr>(
          vCastType, vCastInputs, nullOnFailure);
    }
    default:
      throw std::runtime_error(
          "Unsupported expression type " +
          std::to_string(sExpr.rex_type_case()));
  }
}

std::shared_ptr<PartitionedOutputNode>
SubstraitVeloxConvertor::transformSDistribute(
    const io::substrait::Plan& sPlan,
    int depth) {
  // TODO
}

velox::RowTypePtr SubstraitVeloxConvertor::sNamedStructToVRowTypePtr(
    io::substrait::Type_NamedStruct sNamedStruct) {
  std::vector<std::string> vNames;
  std::vector<velox::TypePtr> vTypes;
  auto sNamedStructSzie = sNamedStruct.index_size();
  for (int64_t i = 0; i < sNamedStructSzie; i++) {
    const io::substrait::Type& sType = sNamedStruct.struct_().types(i);
    velox::TypePtr vType = substraitTypeToVelox(sType);
    std::string sName = sNamedStruct.names(i);
    vNames.emplace_back(sName);
    vTypes.emplace_back(vType);
  }

  std::shared_ptr<const RowType> vRowTypeRes =
      ROW(std::move(vNames), std::move(vTypes));
  return vRowTypeRes;
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
  std::shared_ptr<const velox::RowType> vOutputType =
      sNamedStructToVRowTypePtr(sRead.base_schema());

  // TODO need to add the impl of type local_files

  if (sRead.has_filter()) {
    return std::make_shared<FilterNode>(
        std::to_string(depth),
        transformSExpr(sRead.filter(), sGlobalMapping),
        fromSubstraitIR(sRel, depth + 1));
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

std::shared_ptr<ProjectNode> SubstraitVeloxConvertor::transformSProject(
    const io::substrait::Rel& sRel,
    int depth) {
  const io::substrait::ProjectRel& sProj = sRel.project();
  std::vector<std::shared_ptr<const ITypedExpr>> vExpressions;
  std::vector<std::string> names;

  std::shared_ptr<const PlanNode> vSource =
      fromSubstraitIR(sProj.input(), depth + 1);

  for (auto& sExpr : sProj.expressions()) {
    std::shared_ptr<const ITypedExpr> vExpr =
        transformSExpr(sExpr, sGlobalMapping);
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

  const io::substrait::AggregateRel& sAgg = sRel.aggregate();
  std::shared_ptr<const PlanNode> vSource =
      fromSubstraitIR(sAgg.input(), depth + 1);

  // TODO need to confirm whether this is only for one grouping set, GROUP BY
  // a,b,c. Not fit for GROUPING SETS ???
  for (auto& sGroup : sAgg.groupings()) {
    for (auto& sExpr : sGroup.grouping_expressions()) {
      std::shared_ptr<const ITypedExpr> vGroupingKey =
          transformSExpr(sExpr, sGlobalMapping);
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
            transformSExpr(sAggMask, sGlobalMapping);
        aggregateMask =
            std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vAggMask);
      }
      aggregateMasks.push_back(aggregateMask);
    }

    std::vector<std::shared_ptr<const ITypedExpr>> children;
    std::string out_name;
    std::string function_name = FindFunction(sMeasure.id().id());
    out_name = function_name;
    // AggregateFunction.args should be one for velox . if not, should do
    // project firstly
    int64_t sMeasureArgSize = sMeasure.args_size();
    // the very simple case for sum(a) need to check if this will contain the
    // situation with maskExpression.
    if (sMeasureArgSize == 1) {
      auto vMeasureArgExpr = transformSExpr(sMeasure.args()[0], sGlobalMapping);
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

std::shared_ptr<OrderByNode> SubstraitVeloxConvertor::transformSSort(
    const io::substrait::Rel& sRel,
    int depth) {
  std::vector<OrderByNode> velox_nodes;
  const io::substrait::SortRel& sSort = sRel.sort();

  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys;
  std::vector<SortOrder> sortingOrders;
  bool isPartial;

  std::shared_ptr<const PlanNode> vSource =
      fromSubstraitIR(sSort.input(), depth + 1);

  isPartial = sSort.common().distribution().d_type() == 0 ? true : false;

  // The supported orders are: ascending nulls first, ascending nulls last,
  // descending nulls first, descending nulls last
  for (const io::substrait::Expression_SortField& sOrderField : sSort.sorts()) {
    // TODO check whether  ssort.common() need to be the node output before
    const io::substrait::Expression sExpr = sOrderField.expr();
    std::shared_ptr<const ITypedExpr> sortingKey =
        transformSExpr(sExpr, sGlobalMapping);
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

// ==================   Produce substrait rel    ==================
// ==================   Public APIs   ==================
/**
 * Source is 1st pos of inserted tree
 * @param planNode
 * @return
 */

void SubstraitVeloxConvertor::toSubstraitIR(
    std::shared_ptr<const PlanNode> vPlan,
    io::substrait::Plan& sPlan) {
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
    transformVFilter(filterNode, sFilterRel, sGlobalMapping);
    relCommon = sFilterRel->mutable_common();
  }
  if (auto aggNode =
          std::dynamic_pointer_cast<const AggregationNode>(vPlanNode)) {
    auto sAggRel = sRel->mutable_aggregate();
    transformVAggregateNode(aggNode, sAggRel, sGlobalMapping);
    relCommon = sAggRel->mutable_common();
  }
  if (auto vValuesNode =
          std::dynamic_pointer_cast<const ValuesNode>(vPlanNode)) {
    io::substrait::ReadRel* sReadRel = sRel->mutable_read();
    transformVValuesNode(vValuesNode, sReadRel);
    relCommon = sReadRel->mutable_common();

    sGlobalMapping->MergeFrom(*sReadRel->mutable_base_schema());
  }
  if (auto vProjNode =
          std::dynamic_pointer_cast<const ProjectNode>(vPlanNode)) {
    io::substrait::ProjectRel* sProjRel = sRel->mutable_project();
    transformVProjNode(vProjNode, sProjRel, sGlobalMapping);
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

io::substrait::Type_NamedStruct*
SubstraitVeloxConvertor::vRowTypePtrToSNamedStruct(
    velox::RowTypePtr vRow,
    io::substrait::Type_NamedStruct* sNamedStruct) {
  int64_t vSize = vRow->size();
  std::vector<std::string> vNames = vRow->names();
  std::vector<std::shared_ptr<const Type>> vTypes = vRow->children();
  int64_t sNamedStructSize = sNamedStruct->index_size();

  for (int64_t i = 0; i < vSize; ++i) {
    std::string vName = vNames.at(i);
    std::shared_ptr<const Type> vType = vTypes.at(i);
    sNamedStruct->add_index(sNamedStructSize + i);
    sNamedStruct->add_names(vName);
    io::substrait::Type* sStruct = sNamedStruct->mutable_struct_()->add_types();

    veloxTypeToSubstrait(vType, sStruct);
  }

  return sNamedStruct;
}

io::substrait::Expression_Literal_Struct*
SubstraitVeloxConvertor::processVeloxNullValueByCount(
    std::shared_ptr<const Type> childType,
    std::optional<vector_size_t> nullCount,
    io::substrait::Expression_Literal_Struct* sLitValue,
    io::substrait::Expression_Literal* sField) {
  for (int64_t i = 0; i < nullCount.value(); i++) {
    sField = sLitValue->add_fields();
    processVeloxNullValue(sField, childType);
  }
  return sLitValue;
}

io::substrait::Expression_Literal*
SubstraitVeloxConvertor::processVeloxNullValue(
    io::substrait::Expression_Literal* sField,
    std::shared_ptr<const Type> childType) {
  switch (childType->kind()) {
    case velox::TypeKind::BOOLEAN: {
      io::substrait::Type_Boolean* nullValue =
          new io::substrait::Type_Boolean();
      nullValue->set_nullability(io::substrait::Type_Nullability_NULLABLE);
      sField->mutable_null()->set_allocated_bool_(nullValue);
      break;
    }
    case velox::TypeKind::TINYINT: {
      io::substrait::Type_I8* nullValue = new io::substrait::Type_I8();
      nullValue->set_nullability(io::substrait::Type_Nullability_NULLABLE);
      sField->mutable_null()->set_allocated_i8(nullValue);
      break;
    }
    case velox::TypeKind::SMALLINT: {
      io::substrait::Type_I16* nullValue = new io::substrait::Type_I16();
      nullValue->set_nullability(io::substrait::Type_Nullability_NULLABLE);
      sField->mutable_null()->set_allocated_i16(nullValue);
      break;
    }
    case velox::TypeKind::INTEGER: {
      io::substrait::Type_I32* nullValue = new io::substrait::Type_I32();
      nullValue->set_nullability(io::substrait::Type_Nullability_NULLABLE);
      sField->mutable_null()->set_allocated_i32(nullValue);
      break;
    }
    case velox::TypeKind::BIGINT: {
      io::substrait::Type_I64* nullValue = new io::substrait::Type_I64();
      nullValue->set_nullability(io::substrait::Type_Nullability_NULLABLE);
      sField->mutable_null()->set_allocated_i64(nullValue);
      break;
    }
    case velox::TypeKind::VARCHAR: {
      io::substrait::Type_VarChar* nullValue =
          new io::substrait::Type_VarChar();
      nullValue->set_nullability(io::substrait::Type_Nullability_NULLABLE);
      sField->mutable_null()->set_allocated_varchar(nullValue);
      break;
    }
    case velox::TypeKind::REAL: {
      io::substrait::Type_FP32* nullValue = new io::substrait::Type_FP32();
      nullValue->set_nullability(io::substrait::Type_Nullability_NULLABLE);
      sField->mutable_null()->set_allocated_fp32(nullValue);
      break;
    }
    case velox::TypeKind::DOUBLE: {
      io::substrait::Type_FP64* nullValue = new io::substrait::Type_FP64();
      nullValue->set_nullability(io::substrait::Type_Nullability_NULLABLE);
      sField->mutable_null()->set_allocated_fp64(nullValue);
      break;
    }
    default: {
      throw std::runtime_error(
          "Unsupported type " + std::string(childType->kindName()));
    }
  }

  return sField;
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

      // to handle the null value. TODO need to confirm
      std::optional<vector_size_t> nullCount = children->getNullCount();
      // should be the same with rowValue->type();
      std::shared_ptr<const Type> childType = children->type();
      switch (childType->kind()) {
        case velox::TypeKind::BOOLEAN: {
          auto childToFlatVec = children->asFlatVector<bool>();
          vector_size_t flatVecSzie = childToFlatVec->size();
          if (nullCount.has_value()) {
            processVeloxNullValueByCount(
                childType, nullCount, sLitValue, sField);
          } else {
            for (int64_t i = 0; i < flatVecSzie; i++) {
              sField = sLitValue->add_fields();
              sField->set_boolean(childToFlatVec->valueAt(i));
            }
          }
          break;
        }
        case velox::TypeKind::TINYINT: {
          auto childToFlatVec = children->asFlatVector<int8_t>();
          vector_size_t flatVecSzie = childToFlatVec->size();

          if (nullCount.has_value()) {
            processVeloxNullValueByCount(
                childType, nullCount, sLitValue, sField);
          } else {
            for (int64_t i = 0; i < flatVecSzie; i++) {
              sField = sLitValue->add_fields();
              sField->set_i8(childToFlatVec->valueAt(i));
            }
          }
          break;
        }
        case velox::TypeKind::SMALLINT: {
          auto childToFlatVec = children->asFlatVector<int16_t>();
          vector_size_t flatVecSzie = childToFlatVec->size();
          if (nullCount.has_value()) {
            processVeloxNullValueByCount(
                childType, nullCount, sLitValue, sField);
          } else {
            for (int64_t i = 0; i < flatVecSzie; i++) {
              sField = sLitValue->add_fields();
              sField->set_i16(childToFlatVec->valueAt(i));
            }
          }
          break;
        }
        case velox::TypeKind::INTEGER: {
          if (nullCount.has_value()) {
            processVeloxNullValueByCount(
                childType, nullCount, sLitValue, sField);
          } else {
            // way1
            auto childToFlatVec = children->asFlatVector<int32_t>();
            vector_size_t flatVecSzie = childToFlatVec->size();
            for (int64_t i = 0; i < flatVecSzie; i++) {
              sField = sLitValue->add_fields();
              sField->set_i32(childToFlatVec->valueAt(i));
            }
          }
          break;
        }
        case velox::TypeKind::BIGINT: {
          auto childToFlatVec = children->asFlatVector<int64_t>();
          vector_size_t flatVecSzie = childToFlatVec->size();
          if (nullCount.has_value()) {
            processVeloxNullValueByCount(
                childType, nullCount, sLitValue, sField);
          } else {
            for (int64_t i = 0; i < flatVecSzie; i++) {
              sField = sLitValue->add_fields();
              sField->set_i64(childToFlatVec->valueAt(i));
            }
          }
          break;
        }
        case velox::TypeKind::REAL: {
          auto childToFlatVec = children->asFlatVector<float_t>();
          vector_size_t flatVecSzie = childToFlatVec->size();
          if (nullCount.has_value()) {
            processVeloxNullValueByCount(
                childType, nullCount, sLitValue, sField);
          } else {
            for (int64_t i = 0; i < flatVecSzie; i++) {
              sField = sLitValue->add_fields();
              sField->set_fp32(childToFlatVec->valueAt(i));
            }
          }
          break;
        }
        case velox::TypeKind::DOUBLE: {
          auto childToFlatVec = children->asFlatVector<double_t>();
          vector_size_t flatVecSzie = childToFlatVec->size();
          if (nullCount.has_value()) {
            processVeloxNullValueByCount(
                childType, nullCount, sLitValue, sField);
          } else {
            for (int64_t i = 0; i < flatVecSzie; i++) {
              sField = sLitValue->add_fields();
              sField->set_fp64(childToFlatVec->valueAt(i));
            }
          }
          break;
        }
        case velox::TypeKind::VARCHAR: {
          auto childToFlatVec = children->asFlatVector<StringView>();
          vector_size_t flatVecSzie = childToFlatVec->size();
          if (nullCount.has_value()) {
            auto tmp0 = children->type();
            processVeloxNullValueByCount(
                childType, nullCount, sLitValue, sField);
          } else {
            for (int64_t i = 0; i < flatVecSzie; i++) {
              sField = sLitValue->add_fields();
              sField->set_var_char(childToFlatVec->valueAt(i));
            }
          }
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
    io::substrait::ProjectRel* sProjRel,
    io::substrait::Type_NamedStruct* sGlobalMapping) {
  const PlanNodeId vId = vProjNode->id();
  std::vector<std::string> vNames = vProjNode->names();
  std::vector<std::shared_ptr<const ITypedExpr>> vProjections =
      vProjNode->projections();
  std::shared_ptr<const PlanNode> vSource = vProjNode->sources()[0];

  const RowTypePtr vOutput = vProjNode->outputType();

  io::substrait::Rel* sProjInput = sProjRel->mutable_input();
  toSubstraitIR(vSource, sProjInput);

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

    transformVExpr(sExpr, vExpr, sGlobalMapping);
    // add outputMapping for each vExpr
    const std::shared_ptr<const Type> vExprType = vExpr->type();
    io::substrait::Type* sOutMappingStructType =
        sNewOutMapping->mutable_struct_()->add_types();
    veloxTypeToSubstrait(vExprType, sOutMappingStructType);

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
      veloxTypeToSubstrait(vExprType, sGlobalMappingStructType);

    } else {
      LOG(WARNING) << "the type haven't added" << std::endl;
    }
  }

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
    io::substrait::FilterRel* sFilter,
    io::substrait::Type_NamedStruct* sGlobalMapping) {
  const PlanNodeId vId = vFilter->id();
  std::shared_ptr<const PlanNode> vSource = vFilter->sources()[0];
  std::shared_ptr<const ITypedExpr> vFilterCondition = vFilter->filter();

  io::substrait::Rel* sFilterInput = sFilter->mutable_input();
  io::substrait::Expression* sFilterCondition = sFilter->mutable_condition();
  //   Build source
  toSubstraitIR(vSource, sFilterInput);
  //   Construct substrait expr
  transformVExpr(sFilterCondition, vFilterCondition, sGlobalMapping);
}

void SubstraitVeloxConvertor::transformVAggregateNode(
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
  toSubstraitIR(vSource, sAggInput);

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
    veloxTypeToSubstrait(vOutputchildType, sOutMappingStructType);
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
    transformVExpr(sAggGroupingExpr, vGroupingKey, sGlobalMapping);
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
      veloxTypeToSubstrait(vAggMaskType, sGlobalMappingStructType);
    }
  }

  for (int64_t i = 0; i < vAggregatesSize; i++) {
    io::substrait::AggregateRel_Measure* sAggMeasures = sAggRel->add_measures();
    std::shared_ptr<const CallTypedExpr> vAggregatesExpr = vAggregates.at(i);
    io::substrait::Expression_AggregateFunction* sAggFunction =
        sAggMeasures->mutable_measure();

    io::substrait::Expression* sAggFunctionExpr = sAggFunction->add_args();
    transformVExpr(sAggFunctionExpr, vAggregatesExpr, sGlobalMapping);

    std::string vFunName = vAggregatesExpr->name();
    int64_t sFunId = registerSFunction(vFunName);
    sAggFunction->mutable_id()->set_id(sFunId);

    std::shared_ptr<const Type> vFunOutputType = vAggregatesExpr->type();
    io::substrait::Type* sAggFunOutputType =
        sAggFunction->mutable_output_type();
    veloxTypeToSubstrait(vFunOutputType, sAggFunOutputType);

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
    veloxTypeToSubstrait(vFunOutputType, sGlobalMappingStructType);

    //  TODO need to verify
    //  transform the mask Expr if have.
    if (vAggregateMasksSize != 0) {
      io::substrait::Expression* sAggFilter = sAggMeasures->mutable_filter();
      // TODO what will happened if the expr is ""?
      std::shared_ptr<const FieldAccessTypedExpr> vAggregateMask =
          vAggregateMasks.at(i);
      if (vAggregateMask.get()) {
        transformVExpr(sAggFilter, vAggregateMask, sGlobalMapping);
      }
    }
  }
}

// Private APIs for making expressions
void SubstraitVeloxConvertor::transformVExpr(
    io::substrait::Expression* sExpr,
    const std::shared_ptr<const ITypedExpr>& vExpr,
    io::substrait::Type_NamedStruct* sGlobalMapping) {
  // TODO
  if (std::shared_ptr<const ConstantTypedExpr> vConstantExpr =
          std::dynamic_pointer_cast<const ConstantTypedExpr>(vExpr)) {
    // Literal
    io::substrait::Expression_Literal* sLiteralExpr = sExpr->mutable_literal();
    transformVConstantExpr(vConstantExpr->value(), sLiteralExpr);
    return;
  } else if (
      auto vCallTypeExpr =
          std::dynamic_pointer_cast<const CallTypedExpr>(vExpr)) {
    std::shared_ptr<const Type> vExprType = vCallTypeExpr->type();
    std::vector<std::shared_ptr<const ITypedExpr>> vCallTypeInputs =
        vCallTypeExpr->inputs();
    std::string vCallTypeExprFunName = vCallTypeExpr->name();
    // different by function names.
    if (vCallTypeExprFunName == "if") {
      io::substrait::Expression_IfThen* sFun = sExpr->mutable_if_then();
      int64_t vCallTypeInputSize = vCallTypeInputs.size();
      for (int64_t i = 0; i < vCallTypeInputSize; i++) {
        std::shared_ptr<const ITypedExpr> vCallTypeInput =
            vCallTypeInputs.at(i);
        // TODO
        //  need to judge according the names in the expr, and then set them to
        //  the if or then or else expr can debug to find when process project
        //  node
      }
    } else if (vCallTypeExprFunName == "switch") {
      io::substrait::Expression_SwitchExpression* sFun =
          sExpr->mutable_switch_expression();
      // TODO
    } else {
      io::substrait::Expression_ScalarFunction* sFun =
          sExpr->mutable_scalar_function();
      // TODO need to change yaml file to register functin, now is dummy.
      // the substrait communcity have changed many in this part...
      int64_t sFunId = registerSFunction(vCallTypeExprFunName);
      LOG(INFO) << "sFunId is " << sFunId << std::endl;
      sFun->mutable_id()->set_id(sFunId);

      for (auto& vArg : vCallTypeInputs) {
        io::substrait::Expression* sArg = sFun->add_args();
        transformVExpr(sArg, vArg, sGlobalMapping);
      }
      io::substrait::Type* sFunType = sFun->mutable_output_type();
      veloxTypeToSubstrait(vExprType, sFunType);
      return;
    }

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

    int64_t sIndex;
    int64_t sGlobMapNameSize = sGlobalMapping->names_size();
    for (int64_t i = 0; i < sGlobMapNameSize; i++) {
      if (sGlobalMapping->names(i) == vExprName) {
        // get the index
        sIndex = sGlobalMapping->index(i);
        break;
      }
    }

    sDirectStruct->set_field(sIndex);

    return;

  } else if (
      auto vCastExpr = std::dynamic_pointer_cast<const CastTypedExpr>(vExpr)) {
    std::shared_ptr<const Type> vExprType = vCastExpr->type();
    std::vector<std::shared_ptr<const ITypedExpr>> vCastTypeInputs =
        vCastExpr->inputs();
    io::substrait::Expression_Cast* sCastExpr = sExpr->mutable_cast();
    veloxTypeToSubstrait(vExprType, sCastExpr->mutable_type());

    for (auto& vArg : vCastTypeInputs) {
      io::substrait::Expression* sExpr = sCastExpr->mutable_input();
      transformVExpr(sExpr, vArg, sGlobalMapping);
    }
    return;

  } else {
    throw std::runtime_error(
        "Unsupport Expr " + vExpr->toString() + "in Substrait");
  }
}

void SubstraitVeloxConvertor::transformVConstantExpr(
    const velox::variant& vConstExpr,
    io::substrait::Expression_Literal* sLiteralExpr) {
  switch (vConstExpr.kind()) {
    case velox::TypeKind::DOUBLE: {
      sLiteralExpr->set_fp64(vConstExpr.value<TypeKind::DOUBLE>());
      break;
    }
    case velox::TypeKind::VARCHAR: {
      std::basic_string<char> vCharValue = vConstExpr.value<StringView>();
      sLiteralExpr->set_allocated_var_char(
          reinterpret_cast<std::string*>(vCharValue.data()));
      break;
    }
    case velox::TypeKind::BIGINT: {
      sLiteralExpr->set_i64(vConstExpr.value<TypeKind::BIGINT>());
      break;
    }
    case velox::TypeKind::INTEGER: {
      sLiteralExpr->set_i32(vConstExpr.value<TypeKind::INTEGER>());
      break;
    }
    case velox::TypeKind::SMALLINT: {
      sLiteralExpr->set_i16(vConstExpr.value<TypeKind::INTEGER>());
      break;
    }
    case velox::TypeKind::TINYINT: {
      sLiteralExpr->set_i8(vConstExpr.value<TypeKind::INTEGER>());
      break;
    }
    case velox::TypeKind::BOOLEAN: {
      sLiteralExpr->set_boolean(vConstExpr.value<TypeKind::BOOLEAN>());
      break;
    }
    case velox::TypeKind::REAL: {
      sLiteralExpr->set_fp32(vConstExpr.value<TypeKind::REAL>());
      break;
    }
    case velox::TypeKind::TIMESTAMP: {
      // TODO
      sLiteralExpr->set_timestamp(
          vConstExpr.value<TypeKind::TIMESTAMP>().getNanos());
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
