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
#include "SubstraitToVeloxType.h"

#include "expression/Expr.h"

namespace facebook::velox::substrait {

velox::TypePtr SubstraitToVeloxTypeConvertor::substraitTypeToVelox(
    const ::substrait::Type& sType) {
  switch (sType.kind_case()) {
    case ::substrait::Type::kFixedBinary:
    case ::substrait::Type::kBinary: {
      return velox::TypePtr(velox::VARBINARY());
    }
    case ::substrait::Type::kString:
    case ::substrait::Type::kFixedChar:
    case ::substrait::Type::kVarchar: {
      return velox::TypePtr(velox::VARCHAR());
    }
    case ::substrait::Type::kI8: {
      return velox::TypePtr(velox::TINYINT());
    }
    case ::substrait::Type::kI16: {
      return velox::TypePtr(velox::SMALLINT());
    }
    case ::substrait::Type::kI32: {
      return velox::TypePtr(velox::INTEGER());
    }
    case ::substrait::Type::kI64: {
      return velox::TypePtr(velox::BIGINT());
    }
    case ::substrait::Type::kBool: {
      return velox::TypePtr(velox::BOOLEAN());
    }
    case ::substrait::Type::kFp32: {
      return velox::TypePtr(velox::REAL());
    }
    case ::substrait::Type::kDecimal:
    case ::substrait::Type::kFp64: {
      return velox::TypePtr(velox::DOUBLE());
    }
    case ::substrait::Type::kTimestamp: {
      return velox::TypePtr(velox::TIMESTAMP());
    }
    case ::substrait::Type::kMap: {
      velox::TypePtr keyType = substraitTypeToVelox(sType.map().key());
      velox::TypePtr valueType = substraitTypeToVelox(sType.map().value());
      return velox::TypePtr(velox::MAP(keyType, valueType));
    }
    case ::substrait::Type::kList: {
      velox::TypePtr listType = substraitTypeToVelox(sType.list().type());
      return velox::TypePtr(velox::ARRAY(listType));
    }
    case ::substrait::Type::kDate:
    case ::substrait::Type::kTime:
    case ::substrait::Type::kIntervalDay:
    case ::substrait::Type::kIntervalYear:
    case ::substrait::Type::kTimestampTz:
    case ::substrait::Type::kStruct:
    case ::substrait::Type::kUuid:
    default:
      throw std::runtime_error(
          "Unsupported type " + std::to_string(sType.kind_case()));

      // ROW  UNKNOWN FUNCTION  OPAQUE(using NativeType = std::shared_prt<void>)
      // INVALID(void)
  }
}

variant SubstraitToVeloxTypeConvertor::transformSLiteralType(
    const ::substrait::Expression_Literal& sLiteralExpr) {
  switch (sLiteralExpr.literal_type_case()) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
      // Mapping the kDecimal in Substrait to DOUBLE in Velox
      return velox::variant(sLiteralExpr.fp64());
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kString: {
      return velox::variant(sLiteralExpr.string());
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kVarChar: {
      return velox::variant(sLiteralExpr.var_char().value());
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kFixedChar: {
      return velox::variant(sLiteralExpr.fixed_char());
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
      return velox::variant(sLiteralExpr.boolean());
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64: {
      return velox::variant(sLiteralExpr.i64());
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32: {
      return velox::variant(sLiteralExpr.i32());
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kI16: {
      return velox::variant(static_cast<int16_t>(sLiteralExpr.i16()));
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kI8: {
      return velox::variant(static_cast<int8_t>(sLiteralExpr.i8()));
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64: {
      return velox::variant(sLiteralExpr.fp64());
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp32: {
      return velox::variant(sLiteralExpr.fp32());
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      ::substrait::Type nullValue = sLiteralExpr.null();
      return processSubstraitLiteralNullType(sLiteralExpr, nullValue);
    }
    default:
      throw std::runtime_error(
          "Unsupported liyeral_type in transformSLiteralType " +
          std::to_string(sLiteralExpr.literal_type_case()));
  }
}

variant SubstraitToVeloxTypeConvertor::processSubstraitLiteralNullType(
    const ::substrait::Expression_Literal& sLiteralExpr,
    ::substrait::Type nullType) {
  switch (nullType.kind_case()) {
    case ::substrait::Type::kDecimal: {
      // mapping to DOUBLE
      return velox::variant(sLiteralExpr.fp64());
    }
    case ::substrait::Type::kString: {
      return velox::variant(sLiteralExpr.string());
    }
    case ::substrait::Type::kBool: {
      return velox::variant(sLiteralExpr.boolean());
    }
    case ::substrait::Type::kI64: {
      return velox::variant(sLiteralExpr.i64());
    }
    case ::substrait::Type::kI32: {
      return velox::variant(sLiteralExpr.i32());
    }
    case ::substrait::Type::kI16: {
      return velox::variant(static_cast<int16_t>(sLiteralExpr.i16()));
    }
    case ::substrait::Type::kI8: {
      return velox::variant(static_cast<int8_t>(sLiteralExpr.i8()));
    }
    case ::substrait::Type::kFp64: {
      return velox::variant(sLiteralExpr.fp64());
    }
    case ::substrait::Type::kFp32: {
      return velox::variant(sLiteralExpr.fp32());
    }
    default:
      throw std::runtime_error(
          "Unsupported type in processSubstraitLiteralNullType " +
          std::to_string(nullType.kind_case()));
  }
}

velox::RowTypePtr SubstraitToVeloxTypeConvertor::sNamedStructToVRowTypePtr(
    ::substrait::NamedStruct sNamedStruct) {
  std::vector<std::string> vNames;
  std::vector<velox::TypePtr> vTypes;
  auto sNamedStructSize = sNamedStruct.names_size();
  for (int64_t i = 0; i < sNamedStructSize; i++) {
    const ::substrait::Type& sType = sNamedStruct.struct_().types(i);
    velox::TypePtr vType = substraitTypeToVelox(sType);
    std::string sName = sNamedStruct.names(i);
    vNames.emplace_back(sName);
    vTypes.emplace_back(vType);
  }

  std::shared_ptr<const RowType> vRowTypeRes =
      ROW(std::move(vNames), std::move(vTypes));
  return vRowTypeRes;
}

} // namespace facebook::velox::substrait
