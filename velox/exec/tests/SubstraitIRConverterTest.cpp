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
  //{BIGINT(), INTEGER(), SMALLINT(), DOUBLE()})};

  //filter("(c2 < 1000) and (c1 between 0.6 and 1.6) and (c0 >= 100)")
  void assertVtoSFilter(
      std::vector<RowVectorPtr> &&vectors,
      const std::string &filter = "c1 % 10  > 0") {
    auto vPlan = PlanBuilder().values(vectors).filter(filter).planNode();

    assertQuery(vPlan, "SELECT * FROM tmp WHERE " + filter);

    auto message = vPlan->toString(true, true);
    std::cout << "vPlan before substrait trans is====================\n" << message << std::endl;


    sIRConver->toSubstraitIR(vPlan, *sPlan);
    std::cout << "sPlan trans from vPlan is ===============" << std::endl;
    sPlan->PrintDebugString();
  }


  //filter("(c2 < 1000) and (c1 between 0.6 and 1.6) and (c0 >= 100)")
  void assertFilter(
      std::vector<RowVectorPtr> &&vectors,
      const std::string &filter = "c1 % 10  > 0") {
    auto vPlan = PlanBuilder().values(vectors).filter(filter).planNode();

    assertQuery(vPlan, "SELECT * FROM tmp WHERE " + filter);

    auto message = vPlan->toString(true, true);
    std::cout << "vPlan before substrait trans is====================\n" << message << std::endl;

    sIRConver->toSubstraitIR(vPlan, *sPlan);
    std::cout << "sPlan after tans from velox filter Plan is ===============" << std::endl;
    sPlan->PrintDebugString();

    std::string serialized;
    if (!sPlan->SerializeToString(&serialized)) {
      throw std::runtime_error("eek");
    }

    //splan2 is the same with sPlan. readback
    io::substrait::Plan splan2;
    splan2.ParseFromString(serialized);
    std::cout << "splan2 is" << std::endl;
    splan2.PrintDebugString();

    std::shared_ptr<const PlanNode> vPlan2 = sIRConver->fromSubstraitIR(splan2);
    auto mesage2 = vPlan2->toString(true, true);
    std::cout << "vPlan2 trans from substrait is================\n" << mesage2 << std::endl;

    assertQuery(vPlan2, "SELECT * FROM tmp WHERE " + filter);

    auto message3 = vPlan2->toString(true, true);
    std::cout << "vPlan2  after trans from substrait and assertQuery  is================\n" << message3 << std::endl;
  }

  void assertProject(std::vector<RowVectorPtr> &&vectors) {
    auto vPlan = PlanBuilder()
        .values(vectors)
        .project(std::vector<std::string>{"c0", "c1", "c0+c1"})
        .planNode();

    assertQuery(vPlan, "SELECT c0, c1 , c0+c1 FROM tmp");

    auto message = vPlan->toString(true, true);
    std::cout << "vPlan before substrait trans is====================\n" << message << std::endl;

    sIRConver->toSubstraitIR(vPlan, *sPlan);
    std::cout << "sPlan is ===============" << std::endl;
    sPlan->PrintDebugString();
    std::string serialized;
    if (!sPlan->SerializeToString(&serialized)) {
      throw std::runtime_error("eek");
    }

    //splan2 is the same with sPlan. readback
    io::substrait::Plan splan2;
    splan2.ParseFromString(serialized);
    std::cout << "splan2 is" << std::endl;
    splan2.PrintDebugString();

    std::shared_ptr<const PlanNode> vPlan2 = sIRConver->fromSubstraitIR(splan2);
    auto mesage2 = vPlan2->toString(true, true);
    std::cout << "vPlan2 trans from substrait is================\n" << mesage2 << std::endl;

    assertQuery(vPlan2, "SELECT c0, c1, c0+c1 FROM tmp");

    std::cout << "vPlan2  after trans from substrait and assertQuery  is================\n" << mesage2 << std::endl;
    vPlan2->toString(true, true);

  }

  void assertVtoSProject(std::vector<RowVectorPtr> &&vectors) {
    auto vPlan = PlanBuilder()
        .values(vectors)
        .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
        .planNode();

    assertQuery(vPlan, "SELECT c0, c1, c0 + c1 FROM tmp");

    auto message = vPlan->toString(true, true);
    std::cout << message << std::endl;

    sIRConver->toSubstraitIR(vPlan, *sPlan);
    std::cout << "sPlan is ===============" << std::endl;
    sPlan->PrintDebugString();
  }

  void assertValues(std::vector<RowVectorPtr> &&vectors) {
    for(int i=0;i<vectors.size(); i++){
      std::cout<<vectors.at(i)<<std::endl;
      std::cout<<vectors.at(i)->childrenSize()<<std::endl;
    }
    auto vPlan = PlanBuilder().values(vectors).planNode();

    auto message = vPlan->toString(true, true);
    std::cout << "vPlan in assertValues before substrait trans is " << message << std::endl;

    sIRConver->toSubstraitIR(vPlan, *sPlan);
    std::cout << "sPlan in assertValues is ===============" << std::endl;
    sPlan->PrintDebugString();

    // readback
    auto vPlan2 = sIRConver->fromSubstraitIR(*sPlan);

    auto mesage2 = vPlan2->toString(true, true);
    std::cout << "vPlan2 in assertValues trans from substrait is================\n" << mesage2 << std::endl;

    io::substrait::Plan *sPlan2 = new io::substrait::Plan();
    sIRConver->toSubstraitIR(vPlan2, *sPlan2);
    std::cout << "sPlan2 in assertValues is ===============" << std::endl;
    sPlan2->PrintDebugString();

  }

  void assertVtoSValues(std::vector<RowVectorPtr> &&vectors) {
    auto vPlan = PlanBuilder().values(vectors).planNode();

    auto message = vPlan->toString(true, true);
    std::cout << message << std::endl;
    sIRConver->toSubstraitIR(vPlan, *sPlan);
    std::cout << "sPlan in assertValues is ===============" << std::endl;
    sPlan->PrintDebugString();
  }

  SubstraitVeloxConvertor *sIRConver = new SubstraitVeloxConvertor();
  io::substrait::Plan *sPlan = new io::substrait::Plan();
};

TEST_F(SubstraitIRConverterTest, values) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 2, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);
  assertValues(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, assertVtoSValues) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 2, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);
  assertVtoSValues(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, project) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 3, *pool_));
    vectors.push_back(vector);
  }

  createDuckDbTable(vectors);

  assertProject(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, vTosproject) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 2, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertVtoSProject(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, vToSFilter) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 2, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertVtoSFilter(std::move(vectors));
}

TEST_F(SubstraitIRConverterTest, filter) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 2, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertFilter(std::move(vectors));
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