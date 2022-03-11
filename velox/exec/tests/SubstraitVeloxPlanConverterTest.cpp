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
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include "velox/exec/SubstraitVeloxPlanConvertor.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substraitconvertor;

using facebook::velox::test::BatchMaker;

class SubstraitVeloxPlanConverterTest : public OperatorTestBase {
 protected:
  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3"},
          {INTEGER(), INTEGER(), INTEGER(), INTEGER()})};

  /*!
   *
   * @param size  the number of RowVectorPtr.
   * @param childSize the size of rowType_, the number of columns
   * @param batchSize batch size.
   * @return std::vector<RowVectorPtr>
   */
  std::vector<RowVectorPtr>
  makeVector(int64_t size, int64_t childSize, int64_t batchSize) {
    std::vector<RowVectorPtr> vectors;
    for (int i = 0; i < size; i++) {
      std::vector<VectorPtr> children;
      for (int j = 0; j < childSize; j++) {
        VectorPtr child = VELOX_DYNAMIC_TYPE_DISPATCH(
            BatchMaker::createVector,
            rowType_->childAt(j)->kind(),
            rowType_->childAt(j),
            batchSize,
            *pool_);
        children.emplace_back(child);
      }

      auto rowVector = std::make_shared<RowVector>(
          pool_.get(), rowType_, BufferPtr(), batchSize, children);
      vectors.emplace_back(rowVector);
    }
    return vectors;
  };

  void assertVeloxSubstraitRoundTripFilter(
      std::vector<RowVectorPtr>&& vectors,
      const std::string& filter =
          "(c2 < 1000) and (c1 between 0.6 and 1.6) and (c0 >= 100)") {
    auto vPlan = PlanBuilder().values(vectors).filter(filter).planNode();

    assertQuery(vPlan, "SELECT * FROM tmp WHERE " + filter);

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in assertVeloxSubstraitRoundTripFilter is: \n"
        << message << std::endl;

    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxSubstraitRoundTripFilter is :"
        << std::endl;
    sPlan->PrintDebugString();

    // Convert back
    std::shared_ptr<const PlanNode> vPlan2 =
        s2VPlanConvertor->substraitIRToVelox(*sPlan);
    auto mesage2 = vPlan2->toString(true, true);
    LOG(INFO)
        << "After transform from substrait, velox plan in assertVeloxSubstraitRoundTripFilter is :\n"
        << mesage2 << std::endl;

    assertQuery(vPlan2, "SELECT * FROM tmp WHERE " + filter);
  }

  void assertVeloxToSubstraitFilter(
      std::vector<RowVectorPtr>&& vectors,
      const std::string& filter =
          "(c2 < 1000) and (c1 between 0.6 and 1.6) and (c0 >= 100)") {
    auto vPlan = PlanBuilder().values(vectors).filter(filter).planNode();

    assertQuery(vPlan, "SELECT * FROM tmp WHERE " + filter);

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in assertVeloxToSubstraitFilter is: \n"
        << message << std::endl;

    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxToSubstraitFilter is :"
        << std::endl;
    sPlan->PrintDebugString();
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

    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxSubstraitRoundTripProject is :"
        << std::endl;
    sPlan->PrintDebugString();

    // convert back
    std::shared_ptr<const PlanNode> vPlan2 =
        s2VPlanConvertor->substraitIRToVelox(*sPlan);
    auto mesage2 = vPlan2->toString(true, true);
    LOG(INFO)
        << "After transform from substrait, velox plan in assertVeloxSubstraitRoundTripProject is :\n"
        << mesage2 << std::endl;
    assertQuery(vPlan2, "SELECT c0, c1, c0 + c1 FROM tmp");
  }

  void assertVeloxToSubstraitProject(std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder()
                     .values(vectors)
                     .project(std::vector<std::string>{"c0 + c1", "c1-c2"})
                     .planNode();

    assertQuery(vPlan, "SELECT  c0 + c1, c1-c2 FROM tmp");

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in assertVeloxToSubstraitProject is: \n"
        << message << std::endl;

    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxToSubstraitProject is :"
        << std::endl;
    sPlan->PrintDebugString();
  }

  void assertFilterProjectFused(std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder()
                     .values(vectors)
                     .project(std::vector<std::string>{
                         "c0", "c1", "c0 % 100 + c1 % 50 AS e1"})
                     .filter("e1 > 13")
                     .planNode();

    assertQuery(vPlan, "SELECT c0, c1 ,c0 % 100 + c1 % 50 AS e1 FROM tmp where c0 % 100 + c1 % 50 >13");

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in assertFilterProjectFused is: \n"
        << message << std::endl;

    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);

    LOG(INFO)
        << "After transform from velox, substrait plan in assertFilterProjectFused is :"
        << std::endl;
    sPlan->PrintDebugString();

    // convert back
    auto vPlan2 = s2VPlanConvertor->substraitIRToVelox(*sPlan);

    auto mesage2 = vPlan2->toString(true, true);
    LOG(INFO)
        << "After transform from substrait, velox plan in assertFilterProjectFused is\n"
        << mesage2 << std::endl;

    assertQuery(vPlan2, "SELECT c0, c1,c0 % 100 + c1 % 50 AS e1 FROM tmp where c0 % 100 + c1 % 50 >13");
    vPlan2->toString(true, true);
  }

  void assertVeloxSubstraitRoundTripFilterProject(
      std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder()
                     .values(vectors)
                     .filter("c1 % 10  > 0")
                     .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
                     .planNode();

    assertQuery(vPlan, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 % 10 > 0");

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in veloxSubstraitRoundTripFilterProject is: \n"
        << message << std::endl;

    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in veloxSubstraitRoundTripFilterProject is :"
        << std::endl;
    sPlan->PrintDebugString();

    // convert back
    auto vPlan2 = s2VPlanConvertor->substraitIRToVelox(*sPlan);

    auto mesage2 = vPlan2->toString(true, true);
    LOG(INFO)
        << "After transform from substrait, velox plan in veloxSubstraitRoundTripFilterProject is\n"
        << mesage2 << std::endl;

    assertQuery(vPlan2, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 % 10 > 0");
    vPlan2->toString(true, true);
  }
  void assertVeloxSubstraitRoundTripValues(
      std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder().values(vectors).planNode();

    auto message = vPlan->toString(true, true);
    LOG(INFO)
        << "Before transform, velox plan in assertVeloxSubstraitRoundTripValues is "
        << message << std::endl;

    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxSubstraitRoundTripValues is :"
        << std::endl;
    sPlan->PrintDebugString();

    // convert back
    auto vPlan2 = s2VPlanConvertor->substraitIRToVelox(*sPlan);

    auto mesage2 = vPlan2->toString(true, true);
    LOG(INFO)
        << "After transform from substrait, velox plan in assertVeloxSubstraitRoundTripValues is\n"
        << mesage2 << std::endl;

    substrait::Plan* sPlan2 = new substrait::Plan();
    v2SPlanConvertor->veloxToSubstraitIR(vPlan2, *sPlan2);
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
    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);
    LOG(INFO)
        << "After transform from velox, substrait plan in assertVeloxToSubstraitValues is :"
        << std::endl;
    sPlan->PrintDebugString();
  }

  void SetUp() override {
    v2SPlanConvertor = new VeloxToSubstraitPlanConvertor();
    s2VPlanConvertor = new SubstraitToVeloxPlanConvertor();
    sPlan = new substrait::Plan();
  }

  void TearDown() override {
    delete v2SPlanConvertor;
    delete s2VPlanConvertor;
    delete sPlan;
  }

  VeloxToSubstraitPlanConvertor* v2SPlanConvertor;
  SubstraitToVeloxPlanConvertor* s2VPlanConvertor;
  substrait::Plan* sPlan;
};

TEST_F(SubstraitVeloxPlanConverterTest, veloxSubstraitRoundTripValuesNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  assertVeloxSubstraitRoundTripValues(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, veloxToSubstraitValuesNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  assertVeloxToSubstraitValues(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, veloxSubstraitRoundTripProjectNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);

  assertVeloxSubstraitRoundTripProject(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, veloxToSubstraitProjectNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);

  assertVeloxToSubstraitProject(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, veloxSubstraitRoundTripFilterNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  assertVeloxSubstraitRoundTripFilter(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, veloxToSubstraitFilterNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  assertVeloxToSubstraitFilter(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, veloxSubstraitRoundTripFilterProject) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);

  assertVeloxSubstraitRoundTripFilterProject(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, filterProjectFused) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);

  assertFilterProjectFused(std::move(vectors));
}