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

#include "SubstraitVeloxExprConvertor.h"

#include "velox/expression/ControlExpr.h"

#include "GlobalCommonVariable.h"

namespace facebook::velox::substraitconvertor {

void VeloxToSubstraitExprConvertor::transformVExpr(
    substrait::Expression* sExpr,
    const std::shared_ptr<const ITypedExpr>& vExpr,
    RowTypePtr vPreNodeOutPut) {
  if (std::shared_ptr<const ConstantTypedExpr> vConstantExpr =
          std::dynamic_pointer_cast<const ConstantTypedExpr>(vExpr)) {
    // Literal
    transformVConstantExpr(vConstantExpr->value(), sExpr->mutable_literal());
    return;
  }
  if (auto vCallTypeExpr =
          std::dynamic_pointer_cast<const CallTypedExpr>(vExpr)) {
    std::shared_ptr<const Type> vExprType = vCallTypeExpr->type();
    std::vector<std::shared_ptr<const ITypedExpr>> vCallTypeInputs =
        vCallTypeExpr->inputs();
    std::string vCallTypeExprFunName = vCallTypeExpr->name();
    // different by function names.
    if (vCallTypeExprFunName == exec::kIf) {
      substrait::Expression_IfThen* sFun = sExpr->mutable_if_then();
      int64_t vCallTypeInputSize = vCallTypeInputs.size();
      for (int64_t i = 0; i < vCallTypeInputSize; i++) {
        std::shared_ptr<const ITypedExpr> vCallTypeInput =
            vCallTypeInputs.at(i);
        // TODO
        //  need to judge according the names in the expr, and then set them to
        //  the if or then or else expr can debug to find when process project
        //  node
      }
    } else if (vCallTypeExprFunName == exec::kSwitch) {
      substrait::Expression_SwitchExpression* sFun =
          sExpr->mutable_switch_expression();
      // TODO
    } else {
      substrait::Expression_ScalarFunction* sFun =
          sExpr->mutable_scalar_function();
      // TODO need to change yaml file to register functin, now is dummy.
      // the substrait communcity have changed many in this part...
      uint32_t sFunId =
          v2SFuncConvertor_.registerSFunction(vCallTypeExprFunName);
      LOG(INFO) << "sFunId is " << sFunId << std::endl;
      sFun->set_function_reference(sFunId);

      for (auto& vArg : vCallTypeInputs) {
        substrait::Expression* sArg = sFun->add_args();
        transformVExpr(sArg, vArg, vPreNodeOutPut);
      }
      substrait::Type* sFunType = sFun->mutable_output_type();
      v2STypeConvertor_.veloxTypeToSubstrait(vExprType, sFunType);
      return;
    }
  }
  if (auto vFieldExpr =
          std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vExpr)) {
    // kSelection
    const std::shared_ptr<const Type> vExprType = vFieldExpr->type();
    std::string vExprName = vFieldExpr->name();

    substrait::Expression_ReferenceSegment_StructField* sDirectStruct =
        sExpr->mutable_selection()
            ->mutable_direct_reference()
            ->mutable_struct_field();

    std::vector<std::string> vPreNodeColNames = vPreNodeOutPut->names();
    std::vector<std::shared_ptr<const velox::Type>> vPreNodeColTypes =
        vPreNodeOutPut->children();
    int64_t vPreNodeColNums = vPreNodeColNames.size();
    int64_t sCurrentColId = -1;

    VELOX_CHECK_EQ(vPreNodeColNums, vPreNodeColTypes.size());

    for (int64_t i = 0; i < vPreNodeColNums; i++) {
      if (vPreNodeColNames[i] == vExprName &&
          vPreNodeColTypes[i] == vExprType) {
        sCurrentColId = i;
        break;
      }
    }
    // cannot go to here???
    if (sCurrentColId == -1) {
      sCurrentColId = vPreNodeColNums + 1;
    }
    sDirectStruct->set_field(sCurrentColId);

    return;
  }
  if (auto vCastExpr = std::dynamic_pointer_cast<const CastTypedExpr>(vExpr)) {
    std::vector<std::shared_ptr<const ITypedExpr>> vCastTypeInputs =
        vCastExpr->inputs();
    substrait::Expression_Cast* sCastExpr = sExpr->mutable_cast();
    v2STypeConvertor_.veloxTypeToSubstrait(
        vCastExpr->type(), sCastExpr->mutable_type());

    for (auto& vArg : vCastTypeInputs) {
      transformVExpr(sCastExpr->mutable_input(), vArg, vPreNodeOutPut);
    }
    return;

  } else {
    VELOX_NYI("Unsupport Expr '{}' in Substrait", vExpr->toString());
  }
}

void VeloxToSubstraitExprConvertor::transformVConstantExpr(
    const velox::variant& vConstExpr,
    substrait::Expression_Literal* sLiteralExpr) {
  switch (vConstExpr.kind()) {
    case velox::TypeKind::DOUBLE: {
      sLiteralExpr->set_fp64(vConstExpr.value<TypeKind::DOUBLE>());
      break;
    }
    case velox::TypeKind::VARCHAR: {
      auto vCharValue = vConstExpr.value<StringView>();
      substrait::Expression_Literal::VarChar* sVarChar =
          new substrait::Expression_Literal::VarChar();
      sVarChar->set_value(vCharValue.data());
      sVarChar->set_length(vCharValue.size());
      sLiteralExpr->set_allocated_var_char(sVarChar);
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
      VELOX_NYI(
          "Unsupported constant Type '{}' ",
          mapTypeKindToName(vConstExpr.kind()));
  }
}

// SubstraitToVeloxExprConvertor
std::shared_ptr<const ITypedExpr> SubstraitToVeloxExprConvertor::transformSExpr(
    const substrait::Expression& sExpr,
    RowTypePtr vPreNodeOutPut) {
  switch (sExpr.rex_type_case()) {
    case substrait::Expression::RexTypeCase::kLiteral: {
      return transformSLiteralExpr(sExpr.literal());
    }
    case substrait::Expression::RexTypeCase::kSelection: {
      // TODO if(sExpr.selection().has_root_reference()){}
      if (!sExpr.selection().has_direct_reference() ||
          !sExpr.selection().direct_reference().has_struct_field()) {
        VELOX_NYI("Can only have direct struct references in selections");
      }

      auto colId = sExpr.selection().direct_reference().struct_field().field();

      std::vector<std::string> vPreNodeColNames = vPreNodeOutPut->names();
      std::vector<std::shared_ptr<const velox::Type>> vPreNodeColTypes =
          vPreNodeOutPut->children();
      int64_t vPreNodeColNums = vPreNodeColNames.size();
      if (colId <= vPreNodeColNums) {
        // convert type to row
        return std::make_shared<FieldAccessTypedExpr>(
            vPreNodeColTypes[colId],
            std::make_shared<InputTypedExpr>(vPreNodeColTypes[colId]),
            vPreNodeColNames[colId]);
      } else {
        VELOX_FAIL("Missing the column with id '{}' .", colId);
      }
    }
    case substrait::Expression::RexTypeCase::kScalarFunction: {
      substrait::Expression_ScalarFunction sScalarFunc =
          sExpr.scalar_function();

      velox::TypePtr vScalaFunType =
          s2VTypeConvertor_.substraitTypeToVelox(sScalarFunc.output_type());

      std::vector<std::shared_ptr<const ITypedExpr>> children;
      children.reserve(sScalarFunc.args_size());
      for (auto& sArg : sScalarFunc.args()) {
        children.push_back(transformSExpr(sArg, vPreNodeOutPut));
      }
      // TODO search function name by yaml extension
      std::string function_name =
          s2VFuncConvertor_.FindFunction(sScalarFunc.function_reference());
      //  and or  try concatrow
      if (function_name != exec::kIf && function_name != exec::kSwitch) {
        return std::make_shared<CallTypedExpr>(
            vScalaFunType, children, function_name);
      }
    }
    case substrait::Expression::RexTypeCase::kIfThen: {
      substrait::Expression_ScalarFunction sScalarFunc =
          sExpr.scalar_function();
      velox::TypePtr vScalaFunType =
          s2VTypeConvertor_.substraitTypeToVelox(sScalarFunc.output_type());

      std::vector<std::shared_ptr<const ITypedExpr>> children;
      children.reserve(sScalarFunc.args_size());
      for (auto& sArg : sScalarFunc.args()) {
        children.push_back(transformSExpr(sArg, vPreNodeOutPut));
      }
      return std::make_shared<velox::core::CallTypedExpr>(
          vScalaFunType, move(children), exec::kIf);
    }
    case substrait::Expression::RexTypeCase::kSwitchExpression: {
      substrait::Expression_ScalarFunction sScalarFunc =
          sExpr.scalar_function();
      velox::TypePtr vScalaFunType =
          s2VTypeConvertor_.substraitTypeToVelox(sScalarFunc.output_type());
      std::vector<std::shared_ptr<const ITypedExpr>> children;
      children.reserve(sScalarFunc.args_size());
      for (auto& sArg : sScalarFunc.args()) {
        children.push_back(transformSExpr(sArg, vPreNodeOutPut));
      }
      return std::make_shared<velox::core::CallTypedExpr>(
          vScalaFunType, move(children), exec::kSwitch);
    }
    case substrait::Expression::kCast: {
      substrait::Expression_Cast sCastExpr = sExpr.cast();
      std::shared_ptr<const Type> vCastType =
          s2VTypeConvertor_.substraitTypeToVelox(sCastExpr.type());

      // TODO add flag in substrait after. now is set false.
      bool nullOnFailure = false;

      std::vector<std::shared_ptr<const ITypedExpr>> vCastInputs;
      vCastInputs.reserve(1);
      std::shared_ptr<const ITypedExpr> vCastInput =
          transformSExpr(sCastExpr.input(), vPreNodeOutPut);
      vCastInputs.emplace_back(vCastInput);

      return std::make_shared<CastTypedExpr>(
          vCastType, vCastInputs, nullOnFailure);
    }
    default:
      VELOX_NYI(
          "Unsupported expression type '{}'",
          std::to_string(sExpr.rex_type_case()));
  }
}

std::shared_ptr<const ITypedExpr>
SubstraitToVeloxExprConvertor::transformSLiteralExpr(
    const substrait::Expression_Literal& sLiteralExpr) {
  return std::make_shared<ConstantTypedExpr>(
      s2VTypeConvertor_.transformSLiteralType(sLiteralExpr));
}

} // namespace facebook::velox::substraitconvertor
