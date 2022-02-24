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

namespace facebook::velox {

io::substrait::Type VeloxToSubstraitTypeConvertor::veloxTypeToSubstrait(
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

io::substrait::Type_NamedStruct*
VeloxToSubstraitTypeConvertor::vRowTypePtrToSNamedStruct(
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

io::substrait::Expression_Literal*
VeloxToSubstraitTypeConvertor::processVeloxValueByType(
    io::substrait::Expression_Literal_Struct* sLitValue,
    io::substrait::Expression_Literal* sField,
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
          sField->set_var_char(childToFlatVec->valueAt(i));
        }
      }
      return sField;
    }
    default:
      throw std::runtime_error(
          "Unsupported type " + std::string(childType->kindName()));
  }
}

io::substrait::Expression_Literal*
VeloxToSubstraitTypeConvertor::processVeloxNullValueByCount(
    std::shared_ptr<const Type> childType,
    std::optional<vector_size_t> nullCount,
    io::substrait::Expression_Literal_Struct* sLitValue,
    io::substrait::Expression_Literal* sField) {
  for (int64_t i = 0; i < nullCount.value(); i++) {
    sField = sLitValue->add_fields();
    processVeloxNullValue(sField, childType);
  }
  return sField;
}

io::substrait::Expression_Literal*
VeloxToSubstraitTypeConvertor::processVeloxNullValue(
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

// SubstraitToVeloxTypeConvertor

velox::TypePtr SubstraitToVeloxTypeConvertor::substraitTypeToVelox(
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

variant SubstraitToVeloxTypeConvertor::transformSLiteralType(
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

variant SubstraitToVeloxTypeConvertor::processSubstraitLiteralNullType(
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

velox::RowTypePtr SubstraitToVeloxTypeConvertor::sNamedStructToVRowTypePtr(
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

} // namespace facebook::velox