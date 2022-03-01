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

namespace facebook::velox {

void VeloxToSubstraitExprConvertor::transformVExpr(
    substrait::Expression* sExpr,
    const std::shared_ptr<const ITypedExpr>& vExpr,
    substrait::NamedStruct* sGlobalMapping) {
  // TODO
  if (std::shared_ptr<const ConstantTypedExpr> vConstantExpr =
          std::dynamic_pointer_cast<const ConstantTypedExpr>(vExpr)) {
    // Literal
    substrait::Expression_Literal* sLiteralExpr = sExpr->mutable_literal();
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
    } else if (vCallTypeExprFunName == "switch") {
      substrait::Expression_SwitchExpression* sFun =
          sExpr->mutable_switch_expression();
      // TODO
    } else {
      substrait::Expression_ScalarFunction* sFun =
          sExpr->mutable_scalar_function();
      // TODO need to change yaml file to register functin, now is dummy.
      // the substrait communcity have changed many in this part...
      int64_t sFunId = v2SFuncConvertor.registerSFunction(vCallTypeExprFunName);
      LOG(INFO) << "sFunId is " << sFunId << std::endl;
      sFun->set_function_reference(sFunId);

      for (auto& vArg : vCallTypeInputs) {
        substrait::Expression* sArg = sFun->add_args();
        transformVExpr(sArg, vArg, sGlobalMapping);
      }
      substrait::Type* sFunType = sFun->mutable_output_type();
      v2STypeConvertor.veloxTypeToSubstrait(vExprType, sFunType);
      return;
    }

  } else if (
      auto vFieldExpr =
          std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vExpr)) {
    // kSelection
    const std::shared_ptr<const Type> vExprType = vFieldExpr->type();
    std::string vExprName = vFieldExpr->name();

    substrait::Expression_ReferenceSegment_StructField* sDirectStruct =
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
    substrait::Expression_Cast* sCastExpr = sExpr->mutable_cast();
    v2STypeConvertor.veloxTypeToSubstrait(vExprType, sCastExpr->mutable_type());

    for (auto& vArg : vCastTypeInputs) {
      substrait::Expression* sExpr = sCastExpr->mutable_input();
      transformVExpr(sExpr, vArg, sGlobalMapping);
    }
    return;

  } else {
    throw std::runtime_error(
        "Unsupport Expr " + vExpr->toString() + "in Substrait");
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
      substrait::Expression_Literal::VarChar* sVarChar = new substrait::Expression_Literal::VarChar();
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
      throw std::runtime_error(
          "Unsupported constant Type" + mapTypeKindToName(vConstExpr.kind()));
  }
}

// SubstraitToVeloxExprConvertor
std::shared_ptr<const ITypedExpr> SubstraitToVeloxExprConvertor::transformSExpr(
    const substrait::Expression& sExpr,
    substrait::NamedStruct* sGlobalMapping) {
  switch (sExpr.rex_type_case()) {
    case substrait::Expression::RexTypeCase::kLiteral: {
      auto slit = sExpr.literal();
      std::shared_ptr<const ITypedExpr> sConstant = transformSLiteralExpr(slit);
      return sConstant;
    }
    case substrait::Expression::RexTypeCase::kSelection: {
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
          velox::TypePtr vType = s2VTypeConvertor.substraitTypeToVelox(sType);
          // convert type to row
          return std::make_shared<FieldAccessTypedExpr>(
              vType, std::make_shared<InputTypedExpr>(vType), sName);
        }
      }
    }
    case substrait::Expression::RexTypeCase::kScalarFunction: {
      substrait::Expression_ScalarFunction sScalarFunc =
          sExpr.scalar_function();
      substrait::Type sScalaFunOutType = sScalarFunc.output_type();
      velox::TypePtr vScalaFunType =
          s2VTypeConvertor.substraitTypeToVelox(sScalaFunOutType);

      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sGlobalMapping));
      }
      // TODO search function name by yaml extension
      std::string function_name =
          s2VFuncConvertor.FindFunction(sExpr.scalar_function().function_reference());
      //  and or  try concatrow
      if (function_name != "if" && function_name != "switch") {
        return std::make_shared<CallTypedExpr>(
            vScalaFunType, children, function_name);
      }
    }
    case substrait::Expression::RexTypeCase::kIfThen: {
      substrait::Expression_ScalarFunction sScalarFunc =
          sExpr.scalar_function();
      substrait::Type sScalaFunOutType = sScalarFunc.output_type();
      velox::TypePtr vScalaFunType =
          s2VTypeConvertor.substraitTypeToVelox(sScalaFunOutType);

      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sGlobalMapping));
      }
      return std::make_shared<velox::core::CallTypedExpr>(
          vScalaFunType, move(children), "if");
    }
    case substrait::Expression::RexTypeCase::kSwitchExpression: {
      substrait::Expression_ScalarFunction sScalarFunc =
          sExpr.scalar_function();
      substrait::Type sScalaFunOutType = sScalarFunc.output_type();
      velox::TypePtr vScalaFunType =
          s2VTypeConvertor.substraitTypeToVelox(sScalaFunOutType);
      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sGlobalMapping));
      }
      return std::make_shared<velox::core::CallTypedExpr>(
          vScalaFunType, move(children), "switch");
    }
    case substrait::Expression::kCast: {
      substrait::Expression_Cast sCastExpr = sExpr.cast();

      substrait::Type sCastType = sCastExpr.type();
      std::shared_ptr<const Type> vCastType =
          s2VTypeConvertor.substraitTypeToVelox(sCastType);

      // TODO add flag in substrait after. now is set false.
      bool nullOnFailure = false;

      std::vector<std::shared_ptr<const ITypedExpr>> vCastInputs;
      substrait::Expression sCastInput = sCastExpr.input();
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

std::shared_ptr<const ITypedExpr>
SubstraitToVeloxExprConvertor::transformSLiteralExpr(
    const substrait::Expression_Literal& sLiteralExpr) {
  variant sLiteralExprVariant =
      s2VTypeConvertor.transformSLiteralType(sLiteralExpr);
  return std::make_shared<ConstantTypedExpr>(sLiteralExprVariant);
}

} // namespace facebook::velox