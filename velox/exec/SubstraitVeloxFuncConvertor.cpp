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

#include "SubstraitVeloxFuncConvertor.h"

#include "GlobalCommonVariable.h"

namespace facebook::velox {

uint64_t VeloxToSubstraitFuncConvertor::registerSFunction(std::string name) {
  GlobalCommonVarSingleton& sGlobSingleton =
      GlobalCommonVarSingleton::getInstance();
  substrait::Plan* sPlanSingleton = sGlobSingleton.getSPlan();
  if (function_map_.find(name) == function_map_.end()) {
    auto function_id = last_function_id++;
    auto sFun = sPlanSingleton->add_mappings()->mutable_function_mapping();
    sFun->mutable_extension_id()->set_id(42);
    sFun->mutable_function_id()->set_id(function_id);
    sFun->set_index(function_id);
    sFun->set_name(name);

    function_map_[name] = function_id;
  }
  sGlobSingleton.setSPlan(sPlanSingleton);
  return function_map_[name];
}

void SubstraitToVeloxFuncConvertor::initFunctionMap() {
  GlobalCommonVarSingleton& sGlobSingleton =
      GlobalCommonVarSingleton::getInstance();
  substrait::Plan* sPlanSingleton = sGlobSingleton.getSPlan();
  std::unordered_map<uint64_t, std::string> funMapSingleton =
      sGlobSingleton.getFunctionsMap();
  for (auto& sMap : sPlanSingleton->mappings()) {
    if (!sMap.has_function_mapping()) {
      continue;
    }
    auto& sFunMap = sMap.function_mapping();
    funMapSingleton[sFunMap.function_id().id()] = sFunMap.name();
  }
  sGlobSingleton.setFunctionsMap(funMapSingleton);
}

std::string SubstraitToVeloxFuncConvertor::FindFunction(uint64_t id) {
  GlobalCommonVarSingleton& sGlobSingleton =
      GlobalCommonVarSingleton::getInstance();
  std::unordered_map<uint64_t, std::string> funMapSingleton =
      sGlobSingleton.getFunctionsMap();
  if (funMapSingleton.find(id) == funMapSingleton.end()) {
    throw std::runtime_error(
        "Could not find function with id: " + std::to_string(id));
  }
  return funMapSingleton[id];
}

} // namespace facebook::velox
