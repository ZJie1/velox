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

//#include "SubstraitIRConverterTest.h"

#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/tests/OperatorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

#include "velox/exec/SubstraitIRConverter.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

class SubstraitIRConverterTest : public OperatorTestBase {
 protected:
  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3"},
          {INTEGER(), INTEGER(), INTEGER(), INTEGER()})};

  void assertVeloxSubstraitRoundTripFilter(
      std::vector<RowVectorPtr>&& vectors,
      const std::string& filter = "c1 % 10  > 0") {
    auto plan = PlanBuilder().values(vectors).filter(filter).planNode();

    assertQuery(plan, "SELECT * FROM tmp WHERE " + filter);
  }

  void assertVeloxSubstraitRoundTripProject(
      std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder()
                     .values(vectors)
                     .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
                     .planNode();

    assertQuery(vPlan, "SELECT c0, c1 , c0 + c1 FROM tmp");

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in assertVeloxSubstraitRoundTripProject is: \n"
        << message << std::endl;

    sIRConver->toSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxSubstraitRoundTripProject is :"
        << std::endl;
    sPlan->PrintDebugString();

    // convert back
    std::shared_ptr<const PlanNode> vPlan2 = sIRConver->fromSubstraitIR(*sPlan);
    auto mesage2 = vPlan2->toString(true, true);
    LOG(INFO)
        << "After transform from substrait, velox plan in assertVeloxSubstraitRoundTripProject is :\n"
        << mesage2 << std::endl;

    assertQuery(vPlan2, "SELECT c0, c1, c0 + c1 FROM tmp");
  }

  void assertVeloxToSubstraitProject(std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder()
                     .values(vectors)
                     .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
                     .planNode();

    assertQuery(vPlan, "SELECT c0, c1, c0 + c1 FROM tmp");

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in assertVeloxToSubstraitProject is: \n"
        << message << std::endl;

    sIRConver->toSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxToSubstraitProject is :"
        << std::endl;
    sPlan->PrintDebugString();
  }

  void assertVeloxSubstraitRoundTripValues(
      std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder().values(vectors).planNode();

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in assertVeloxSubstraitRoundTripValues is "
        << message << std::endl;

    sIRConver->toSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxSubstraitRoundTripValues is :"
        << std::endl;
    sPlan->PrintDebugString();

    // convert back
    auto vPlan2 = sIRConver->fromSubstraitIR(*sPlan);

    auto mesage2 = vPlan2->toString(true, true);
    LOG(INFO)
        << "After transform from substrait, velox plan in assertVeloxSubstraitRoundTripValues is\n"
        << mesage2 << std::endl;

    io::substrait::Plan* sPlan2 = new io::substrait::Plan();
    sIRConver->toSubstraitIR(vPlan2, *sPlan2);
    LOG(INFO)
        << "After transform from velox again, substrait plan in assertVeloxSubstraitRoundTripValues is "
        << std::endl;
    sPlan2->PrintDebugString();
  }

  void assertVeloxToSubstraitValues(std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder().values(vectors).planNode();

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in assertVeloxToSubstraitValues is: \n"
        << message << std::endl;
    sIRConver->toSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxToSubstraitValues is :"
        << std::endl;
    sPlan->PrintDebugString();
  }

  SubstraitVeloxConvertor* sIRConver = new SubstraitVeloxConvertor();
  io::substrait::Plan* sPlan = new io::substrait::Plan();
};

TEST_F(SubstraitIRConverterTest, veloxSubstraitRoundTripValuesNode) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 2, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);
  assertVeloxSubstraitRoundTripValues(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, veloxToSubstraitValuesNode) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 2, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);
  assertVeloxToSubstraitValues(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, veloxSubstraitRoundTripProjectNode) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 2, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertVeloxSubstraitRoundTripProject(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, veloxToSubstraitProjectNode) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 2, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertVeloxToSubstraitProject(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, veloxSubstraitRoundTripFilterNode) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertVeloxSubstraitRoundTripFilter(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, filterProject) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c1 % 10  > 0")
                  .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
                  .planNode();

  assertQuery(plan, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 % 10 > 0");
}