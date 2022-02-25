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
      int64_t sFunId = v2SFuncConvertor.registerSFunction(vCallTypeExprFunName);
      LOG(INFO) << "sFunId is " << sFunId << std::endl;
      sFun->mutable_id()->set_id(sFunId);

      for (auto& vArg : vCallTypeInputs) {
        io::substrait::Expression* sArg = sFun->add_args();
        transformVExpr(sArg, vArg, sGlobalMapping);
      }
      io::substrait::Type* sFunType = sFun->mutable_output_type();
      v2STypeConvertor.veloxTypeToSubstrait(vExprType, sFunType);
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
    v2STypeConvertor.veloxTypeToSubstrait(vExprType, sCastExpr->mutable_type());

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

void VeloxToSubstraitExprConvertor::transformVConstantExpr(
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

// SubstraitToVeloxExprConvertor
std::shared_ptr<const ITypedExpr> SubstraitToVeloxExprConvertor::transformSExpr(
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
          velox::TypePtr vType = s2VTypeConvertor.substraitTypeToVelox(sType);
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
      velox::TypePtr vScalaFunType =
          s2VTypeConvertor.substraitTypeToVelox(sScalaFunOutType);

      std::vector<std::shared_ptr<const ITypedExpr>> children;
      for (auto& sArg : sExpr.scalar_function().args()) {
        children.push_back(transformSExpr(sArg, sGlobalMapping));
      }
      // TODO search function name by yaml extension
      std::string function_name =
          s2VFuncConvertor.FindFunction(sExpr.scalar_function().id().id());
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
      velox::TypePtr vScalaFunType =
          s2VTypeConvertor.substraitTypeToVelox(sScalaFunOutType);

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
      velox::TypePtr vScalaFunType =
          s2VTypeConvertor.substraitTypeToVelox(sScalaFunOutType);
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
      std::shared_ptr<const Type> vCastType =
          s2VTypeConvertor.substraitTypeToVelox(sCastType);

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

std::shared_ptr<const ITypedExpr>
SubstraitToVeloxExprConvertor::transformSLiteralExpr(
    const io::substrait::Expression_Literal& sLiteralExpr) {
  variant sLiteralExprVariant =
      s2VTypeConvertor.transformSLiteralType(sLiteralExpr);
  return std::make_shared<ConstantTypedExpr>(sLiteralExprVariant);
}

} // namespace facebook::velox