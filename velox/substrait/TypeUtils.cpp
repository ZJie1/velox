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

#include "velox/type/Type.h"

namespace facebook::velox::substrait {

bool isString(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return true;
    default:
      break;
  }
  return false;
}

int64_t bytesOfType(const TypePtr& type) {
  auto typeKind = type->kind();
  switch (typeKind) {
    case TypeKind::INTEGER:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return 8;
    default:
      VELOX_NYI("Returning bytes of Type not supported for type {}.", typeKind);
  }
}

void getTypesFromCompoundName(
    const std::string& compoundName,
    std::vector<std::string>& types) {
  types.clear();
  // Get the position of ":" in the compoundName.
  std::size_t pos = compoundName.find(":");
  // Get the parameter types.
  std::string funcTypes;
  if (pos == std::string::npos) {
    return;
  } else {
    if (pos == compoundName.size() - 1) {
      return;
    }
    funcTypes = compoundName.substr(pos + 1);
  }
  // Split the types with delimiter.
  std::string delimiter = "_";
  while ((pos = funcTypes.find(delimiter)) != std::string::npos) {
    types.emplace_back(funcTypes.substr(0, pos));
    funcTypes.erase(0, pos + delimiter.length());
  }
  types.emplace_back(funcTypes);
}

TypePtr toVeloxType(const std::string& typeName) {
  auto typeKind = mapNameToTypeKind(typeName);
  switch (typeKind) {
    case TypeKind::BOOLEAN:
      return BOOLEAN();
    case TypeKind::DOUBLE:
      return DOUBLE();
    case TypeKind::VARCHAR:
      return VARCHAR();
    case TypeKind::INTEGER:
      return INTEGER();
    case TypeKind::BIGINT:
      return BIGINT();
    case TypeKind::UNKNOWN:
      return UNKNOWN();
    case TypeKind::ROW: {
      std::vector<std::string> structTypeNames;
      getTypesFromCompoundName(typeName, structTypeNames);
      VELOX_CHECK(
          structTypeNames.size() > 0, "At lease one type name is expected.");
      // Preparation for the conversion from struct types to RowType.
      std::vector<TypePtr> rowTypes;
      std::vector<std::string> names;
      for (int idx = 0; idx < structTypeNames.size(); idx++) {
        std::string substraitTypeName = structTypeNames[idx];
        names.emplace_back("col_" + std::to_string(idx));
        rowTypes.emplace_back(std::move(toVeloxType(substraitTypeName)));
      }
      return ROW(std::move(names), std::move(rowTypes));
    }
    default:
      VELOX_NYI("Velox type conversion not supported for type {}.", typeName);
  }
}

} // namespace facebook::velox::substrait
