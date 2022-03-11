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
#include "SubstraitVeloxTypeConvertor.h"

#include "expression/Expr.h"

namespace facebook::velox::substraitconvertor  {

substrait::Type VeloxToSubstraitTypeConvertor::veloxTypeToSubstrait(
    const velox::TypePtr& vType,
    substrait::Type* sType) {
  switch (vType->kind()) {
    case velox::TypeKind::BOOLEAN: {
      sType->set_allocated_bool_(new substrait::Type_Boolean());
      return *sType;
    }
    case velox::TypeKind::TINYINT: {
      sType->set_allocated_i8(new substrait::Type_I8());
      return *sType;
    }
    case velox::TypeKind::SMALLINT: {
      sType->set_allocated_i16(new substrait::Type_I16());
      return *sType;
    }
    case velox::TypeKind::INTEGER: {
      sType->set_allocated_i32(new substrait::Type_I32());
      return *sType;
    }
    case velox::TypeKind::BIGINT: {
      sType->set_allocated_i64(new substrait::Type_I64());
      return *sType;
    }
    case velox::TypeKind::REAL: {
      sType->set_allocated_fp32(new substrait::Type_FP32());
      return *sType;
    }
    case velox::TypeKind::DOUBLE: {
      sType->set_allocated_fp64(new substrait::Type_FP64());
      return *sType;
    }
    case velox::TypeKind::VARCHAR: {
      sType->set_allocated_varchar(new substrait::Type_VarChar());
      return *sType;
    }
    case velox::TypeKind::VARBINARY: {
      sType->set_allocated_binary(new substrait::Type_Binary());
      return *sType;
    }
    case velox::TypeKind::TIMESTAMP: {
      sType->set_allocated_timestamp(new substrait::Type_Timestamp());
      return *sType;
    }
    case velox::TypeKind::ARRAY: {
      substrait::Type_List* sTList = new substrait::Type_List();
      const std::shared_ptr<const Type> vArrayType =
          vType->asArray().elementType();
      substrait::Type sListType =
          veloxTypeToSubstrait(vArrayType, sTList->mutable_type());

      sType->set_allocated_list(sTList);
      return *sType;
    }
    case velox::TypeKind::MAP: {
      substrait::Type_Map* sMap = new substrait::Type_Map();
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

substrait::NamedStruct*
VeloxToSubstraitTypeConvertor::vRowTypePtrToSNamedStruct(
    velox::RowTypePtr vRow,
    substrait::NamedStruct* sNamedStruct) {
  int64_t vSize = vRow->size();
  std::vector<std::string> vNames = vRow->names();
  std::vector<std::shared_ptr<const Type>> vTypes = vRow->children();

  for (int64_t i = 0; i < vSize; ++i) {
    std::string vName = vNames.at(i);
    std::shared_ptr<const Type> vType = vTypes.at(i);
    sNamedStruct->add_names(vName);
    substrait::Type* sStruct = sNamedStruct->mutable_struct_()->add_types();

    veloxTypeToSubstrait(vType, sStruct);
  }

  return sNamedStruct;
}

substrait::Expression_Literal*
VeloxToSubstraitTypeConvertor::processVeloxValueByType(
    substrait::Expression_Literal_Struct* sLitValue,
    substrait::Expression_Literal* sField,
    VectorPtr children) {
  // to handle the null value. TODO need to confirm
  std::optional<vector_size_t> nullCount = children->getNullCount();
  // should be the same with rowValue->type();
  std::shared_ptr<const Type> childType = children->type();
  switch (childType->kind()) {
    case velox::TypeKind::BOOLEAN: {
      auto childToFlatVec = children->asFlatVector<bool>();
      vector_size_t flatVecSzie = childToFlatVec->size();
      if (nullCount.has_value()) {
        sField = processVeloxNullValueByCount(
            childType, nullCount, sLitValue, sField);
      } else {
        for (int64_t i = 0; i < flatVecSzie; i++) {
          sField = sLitValue->add_fields();
          sField->set_boolean(childToFlatVec->valueAt(i));
        }
      }
      return sField;
    }
    case velox::TypeKind::TINYINT: {
      auto childToFlatVec = children->asFlatVector<int8_t>();
      vector_size_t flatVecSzie = childToFlatVec->size();

      if (nullCount.has_value()) {
        sField = processVeloxNullValueByCount(
            childType, nullCount, sLitValue, sField);
      } else {
        for (int64_t i = 0; i < flatVecSzie; i++) {
          sField = sLitValue->add_fields();
          sField->set_i8(childToFlatVec->valueAt(i));
        }
      }
      return sField;
    }
    case velox::TypeKind::SMALLINT: {
      auto childToFlatVec = children->asFlatVector<int16_t>();
      vector_size_t flatVecSzie = childToFlatVec->size();
      if (nullCount.has_value()) {
        sField = processVeloxNullValueByCount(
            childType, nullCount, sLitValue, sField);
      } else {
        for (int64_t i = 0; i < flatVecSzie; i++) {
          sField = sLitValue->add_fields();
          sField->set_i16(childToFlatVec->valueAt(i));
        }
      }
      return sField;
    }
    case velox::TypeKind::INTEGER: {
      if (nullCount.has_value()) {
        sField = processVeloxNullValueByCount(
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
      return sField;
    }
    case velox::TypeKind::BIGINT: {
      auto childToFlatVec = children->asFlatVector<int64_t>();
      vector_size_t flatVecSzie = childToFlatVec->size();
      if (nullCount.has_value()) {
        sField = processVeloxNullValueByCount(
            childType, nullCount, sLitValue, sField);
      } else {
        for (int64_t i = 0; i < flatVecSzie; i++) {
          sField = sLitValue->add_fields();
          sField->set_i64(childToFlatVec->valueAt(i));
        }
      }
      return sField;
    }
    case velox::TypeKind::REAL: {
      auto childToFlatVec = children->asFlatVector<float_t>();
      vector_size_t flatVecSzie = childToFlatVec->size();
      if (nullCount.has_value()) {
        sField = processVeloxNullValueByCount(
            childType, nullCount, sLitValue, sField);
      } else {
        for (int64_t i = 0; i < flatVecSzie; i++) {
          sField = sLitValue->add_fields();
          sField->set_fp32(childToFlatVec->valueAt(i));
        }
      }
      return sField;
    }
    case velox::TypeKind::DOUBLE: {
      auto childToFlatVec = children->asFlatVector<double_t>();
      vector_size_t flatVecSzie = childToFlatVec->size();
      if (nullCount.has_value()) {
        sField = processVeloxNullValueByCount(
            childType, nullCount, sLitValue, sField);
      } else {
        for (int64_t i = 0; i < flatVecSzie; i++) {
          sField = sLitValue->add_fields();
          sField->set_fp64(childToFlatVec->valueAt(i));
        }
      }
      return sField;
    }
    case velox::TypeKind::VARCHAR: {
      auto childToFlatVec = children->asFlatVector<StringView>();
      vector_size_t flatVecSzie = childToFlatVec->size();
      if (nullCount.has_value()) {
        auto tmp0 = children->type();
        sField = processVeloxNullValueByCount(
            childType, nullCount, sLitValue, sField);
      } else {
        for (int64_t i = 0; i < flatVecSzie; i++) {
          sField = sLitValue->add_fields();
          substrait::Expression_Literal::VarChar* sVarChar = new substrait::Expression_Literal::VarChar();
          StringView vChildValueAt = childToFlatVec->valueAt(i);
          sVarChar->set_value(vChildValueAt);
          sVarChar->set_length(vChildValueAt.size());
          sField->set_allocated_var_char(sVarChar);
        }
      }
      return sField;
    }
    default:
      throw std::runtime_error(
          "Unsupported type " + std::string(childType->kindName()));
  }
}

substrait::Expression_Literal*
VeloxToSubstraitTypeConvertor::processVeloxNullValueByCount(
    std::shared_ptr<const Type> childType,
    std::optional<vector_size_t> nullCount,
    substrait::Expression_Literal_Struct* sLitValue,
    substrait::Expression_Literal* sField) {
  for (int64_t i = 0; i < nullCount.value(); i++) {
    sField = sLitValue->add_fields();
    processVeloxNullValue(sField, childType);
  }
  return sField;
}

substrait::Expression_Literal*
VeloxToSubstraitTypeConvertor::processVeloxNullValue(
    substrait::Expression_Literal* sField,
    std::shared_ptr<const Type> childType) {
  switch (childType->kind()) {
    case velox::TypeKind::BOOLEAN: {
      substrait::Type_Boolean* nullValue =
          new substrait::Type_Boolean();
      nullValue->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
      sField->mutable_null()->set_allocated_bool_(nullValue);
      break;
    }
    case velox::TypeKind::TINYINT: {
      substrait::Type_I8* nullValue = new substrait::Type_I8();
      nullValue->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
      sField->mutable_null()->set_allocated_i8(nullValue);
      break;
    }
    case velox::TypeKind::SMALLINT: {
      substrait::Type_I16* nullValue = new substrait::Type_I16();
      nullValue->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
      sField->mutable_null()->set_allocated_i16(nullValue);
      break;
    }
    case velox::TypeKind::INTEGER: {
      substrait::Type_I32* nullValue = new substrait::Type_I32();
      nullValue->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
      sField->mutable_null()->set_allocated_i32(nullValue);
      break;
    }
    case velox::TypeKind::BIGINT: {
      substrait::Type_I64* nullValue = new substrait::Type_I64();
      nullValue->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
      sField->mutable_null()->set_allocated_i64(nullValue);
      break;
    }
    case velox::TypeKind::VARCHAR: {
      substrait::Type_VarChar* nullValue =
          new substrait::Type_VarChar();
      nullValue->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
      sField->mutable_null()->set_allocated_varchar(nullValue);
      break;
    }
    case velox::TypeKind::REAL: {
      substrait::Type_FP32* nullValue = new substrait::Type_FP32();
      nullValue->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
      sField->mutable_null()->set_allocated_fp32(nullValue);
      break;
    }
    case velox::TypeKind::DOUBLE: {
      substrait::Type_FP64* nullValue = new substrait::Type_FP64();
      nullValue->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
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

// SubstraitToVeloxTypeConvertor

velox::TypePtr SubstraitToVeloxTypeConvertor::substraitTypeToVelox(
    const substrait::Type& sType) {
  switch (sType.kind_case()) {
    case substrait::Type::kFixedBinary:
    case substrait::Type::kBinary: {
      return velox::TypePtr(velox::VARBINARY());
    }
    case substrait::Type::kString:
    case substrait::Type::kFixedChar:
    case substrait::Type::kVarchar: {
      return velox::TypePtr(velox::VARCHAR());
    }
    case substrait::Type::kI8: {
      return velox::TypePtr(velox::TINYINT());
    }
    case substrait::Type::kI16: {
      return velox::TypePtr(velox::SMALLINT());
    }
    case substrait::Type::kI32: {
      return velox::TypePtr(velox::INTEGER());
    }
    case substrait::Type::kI64: {
      return velox::TypePtr(velox::BIGINT());
    }
    case substrait::Type::kBool: {
      return velox::TypePtr(velox::BOOLEAN());
    }
    case substrait::Type::kFp32: {
      return velox::TypePtr(velox::REAL());
    }
    case substrait::Type::kDecimal:
    case substrait::Type::kFp64: {
      return velox::TypePtr(velox::DOUBLE());
    }
    case substrait::Type::kTimestamp: {
      return velox::TypePtr(velox::TIMESTAMP());
    }
    case substrait::Type::kMap: {
      velox::TypePtr keyType = substraitTypeToVelox(sType.map().key());
      velox::TypePtr valueType = substraitTypeToVelox(sType.map().value());
      return velox::TypePtr(velox::MAP(keyType, valueType));
    }
    case substrait::Type::kList: {
      velox::TypePtr listType = substraitTypeToVelox(sType.list().type());
      return velox::TypePtr(velox::ARRAY(listType));
    }
    case substrait::Type::kDate:
    case substrait::Type::kTime:
    case substrait::Type::kIntervalDay:
    case substrait::Type::kIntervalYear:
    case substrait::Type::kTimestampTz:
    case substrait::Type::kStruct:
    case substrait::Type::kUuid:
    default:
      throw std::runtime_error(
          "Unsupported type " + std::to_string(sType.kind_case()));

      // ROW  UNKNOWN FUNCTION  OPAQUE(using NativeType = std::shared_prt<void>)
      // INVALID(void)
  }
}

variant SubstraitToVeloxTypeConvertor::transformSLiteralType(
    const substrait::Expression_Literal& sLiteralExpr) {
  switch (sLiteralExpr.literal_type_case()) {
    case substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
      // Mapping the kDecimal in Substrait to DOUBLE in Velox
      return velox::variant(sLiteralExpr.fp64());
    }
    case substrait::Expression_Literal::LiteralTypeCase::kString: {
      return velox::variant(sLiteralExpr.string());
    }
    case substrait::Expression_Literal::LiteralTypeCase::kVarChar: {
      return velox::variant(sLiteralExpr.var_char().value());
    }
    case substrait::Expression_Literal::LiteralTypeCase::kFixedChar: {
      return velox::variant(sLiteralExpr.fixed_char());
    }
    case substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
      return velox::variant(sLiteralExpr.boolean());
    }
    case substrait::Expression_Literal::LiteralTypeCase::kI64: {
      return velox::variant(sLiteralExpr.i64());
    }
    case substrait::Expression_Literal::LiteralTypeCase::kI32: {
      return velox::variant(sLiteralExpr.i32());
    }
    case substrait::Expression_Literal::LiteralTypeCase::kI16: {
      return velox::variant(static_cast<int16_t>(sLiteralExpr.i16()));
    }
    case substrait::Expression_Literal::LiteralTypeCase::kI8: {
      return velox::variant(static_cast<int8_t>(sLiteralExpr.i8()));
    }
    case substrait::Expression_Literal::LiteralTypeCase::kFp64: {
      return velox::variant(sLiteralExpr.fp64());
    }
    case substrait::Expression_Literal::LiteralTypeCase::kFp32: {
      return velox::variant(sLiteralExpr.fp32());
    }
    case substrait::Expression_Literal::LiteralTypeCase::kNull: {
      substrait::Type nullValue = sLiteralExpr.null();
      return processSubstraitLiteralNullType(sLiteralExpr, nullValue);
    }
    default:
      throw std::runtime_error(
          "Unsupported liyeral_type in transformSLiteralType " +
          std::to_string(sLiteralExpr.literal_type_case()));
  }
}

variant SubstraitToVeloxTypeConvertor::processSubstraitLiteralNullType(
    const substrait::Expression_Literal& sLiteralExpr,
    substrait::Type nullType) {
  switch (nullType.kind_case()) {
    case substrait::Type::kDecimal: {
      // mapping to DOUBLE
      return velox::variant(sLiteralExpr.fp64());
    }
    case substrait::Type::kString: {
      return velox::variant(sLiteralExpr.string());
    }
    case substrait::Type::kBool: {
      return velox::variant(sLiteralExpr.boolean());
    }
    case substrait::Type::kI64: {
      return velox::variant(sLiteralExpr.i64());
    }
    case substrait::Type::kI32: {
      return velox::variant(sLiteralExpr.i32());
    }
    case substrait::Type::kI16: {
      return velox::variant(static_cast<int16_t>(sLiteralExpr.i16()));
    }
    case substrait::Type::kI8: {
      return velox::variant(static_cast<int8_t>(sLiteralExpr.i8()));
    }
    case substrait::Type::kFp64: {
      return velox::variant(sLiteralExpr.fp64());
    }
    case substrait::Type::kFp32: {
      return velox::variant(sLiteralExpr.fp32());
    }
    default:
      throw std::runtime_error(
          "Unsupported type in processSubstraitLiteralNullType " +
          std::to_string(nullType.kind_case()));
  }
}

velox::RowTypePtr SubstraitToVeloxTypeConvertor::sNamedStructToVRowTypePtr(
    substrait::NamedStruct sNamedStruct) {
  std::vector<std::string> vNames;
  std::vector<velox::TypePtr> vTypes;
  auto sNamedStructSize = sNamedStruct.names_size();
  for (int64_t i = 0; i < sNamedStructSize; i++) {
    const substrait::Type& sType = sNamedStruct.struct_().types(i);
    velox::TypePtr vType = substraitTypeToVelox(sType);
    std::string sName = sNamedStruct.names(i);
    vNames.emplace_back(sName);
    vTypes.emplace_back(vType);
  }

  std::shared_ptr<const RowType> vRowTypeRes =
      ROW(std::move(vNames), std::move(vTypes));
  return vRowTypeRes;
}

} // namespace facebook::velox::substraitconvertor
