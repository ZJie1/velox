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
#include <folly/Random.h>

#include "velox/aggregates/tests/AggregationTestBase.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/tests/PlanBuilder.h"

#include "velox/exec/SubstraitVeloxPlanConvertor.h"

using namespace facebook::velox::aggregate;
using namespace facebook::velox::aggregate::test;

using facebook::velox::test::BatchMaker;

namespace facebook::velox::exec::test {
namespace {

class SubstraitAggregationTest : public AggregationTestBase {
 protected:
  void assertQueryInTestSingleKey(
      std::shared_ptr<PlanNode>& vPlan,
      const std::string& keyName,
      bool ignoreNullKeys,
      bool distinct) {
    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE " + keyName + " IS NOT NULL";
    }
    if (distinct) {
      assertQuery(vPlan, "SELECT distinct " + keyName + " " + fromClause);
    } else {
      assertQuery(
          vPlan,
          "SELECT " + keyName +
              ", sum(15), sum(cast(0.1 as double)), sum(c1), sum(c2), sum(c4), sum(c5) , min(15), min(0.1), min(c1), min(c2), min(c3), min(c4), min(c5), max(15), max(0.1), max(c1), max(c2), max(c3), max(c4), max(c5) " +
              fromClause + " GROUP BY " + keyName);
    }
  }

  void assertQueryInTestMultiKey(
      std::shared_ptr<PlanNode>& vPlan,
      bool ignoreNullKeys,
      bool distinct) {
    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause +=
          " WHERE c0 IS NOT NULL AND c1 IS NOT NULL AND c6 IS NOT NULL";
    }
    if (distinct) {
      assertQuery(vPlan, "SELECT distinct c0, c1, c6 " + fromClause);
    } else {
      assertQuery(
          vPlan,
          "SELECT c0, c1, c6, sum(15), sum(cast(0.1 as double)), sum(c4), sum(c5), min(15), min(0.1), min(c3), min(c4), min(c5), max(15), max(0.1), max(c3), max(c4), max(c5) " +
              fromClause + " GROUP BY c0, c1, c6");
    }
  }

  template <typename T>
  void testVeloxToSubstraitSingleKey(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& keyName,
      bool ignoreNullKeys,
      bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {"sum(15)", "sum(0.1)", "sum(c1)",  "sum(c2)", "sum(c4)",
                    "sum(c5)", "min(15)",  "min(0.1)", "min(c1)", "min(c2)",
                    "min(c3)", "min(c4)",  "min(c5)",  "max(15)", "max(0.1)",
                    "max(c1)", "max(c2)",  "max(c3)",  "max(c4)", "max(c5)"};
    }

    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {rowType_->getChildIdx(keyName)},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();
    assertQueryInTestSingleKey(op, keyName, ignoreNullKeys, distinct);
    testPlanConvertorFromVelox(op, "testVeloxToSubstraitSingleKey");
  }

  template <typename T>
  void testveloxSubstraitRoundTripSingleKey(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& keyName,
      bool ignoreNullKeys,
      bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {"sum(15)", "sum(0.1)", "sum(c1)",  "sum(c2)", "sum(c4)",
                    "sum(c5)", "min(15)",  "min(0.1)", "min(c1)", "min(c2)",
                    "min(c3)", "min(c4)",  "min(c5)",  "max(15)", "max(0.1)",
                    "max(c1)", "max(c2)",  "max(c3)",  "max(c4)", "max(c5)"};
    }

    std::shared_ptr<PlanNode> op =
        PlanBuilder()
            .values(vectors)
            .aggregation(
                {rowType_->getChildIdx(keyName)},
                aggregates,
                {},
                core::AggregationNode::Step::kPartial,
                ignoreNullKeys)
            .planNode();

    assertQueryInTestSingleKey(op, keyName, ignoreNullKeys, distinct);

    std::shared_ptr<const PlanNode> vPlan2 =
        testRoundTripPlanConvertor(op, "testveloxSubstraitRoundTripSingleKey");
    std::shared_ptr<PlanNode> op2 = std::const_pointer_cast<PlanNode>(vPlan2);
    assertQueryInTestSingleKey(op2, keyName, ignoreNullKeys, distinct);
  }
  void testVeloxToSubstraitMultiKey(
      const std::vector<RowVectorPtr>& vectors,
      bool ignoreNullKeys,
      bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {
          "sum(15)",
          "sum(0.1)",
          "sum(c4)",
          "sum(c5)",
          "min(15)",
          "min(0.1)",
          "min(c3)",
          "min(c4)",
          "min(c5)",
          "max(15)",
          "max(0.1)",
          "max(c3)",
          "max(c4)",
          "max(c5)"};
    }
    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {0, 1, 6},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();
    assertQueryInTestMultiKey(op, ignoreNullKeys, distinct);

    // transform to substrait plan
    testPlanConvertorFromVelox(op, "testMultiKey");
  }

  void testVeloxSubstraitRoundTripMultiKey(
      const std::vector<RowVectorPtr>& vectors,
      bool ignoreNullKeys,
      bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {
          "sum(15)",
          "sum(0.1)",
          "sum(c4)",
          "sum(c5)",
          "min(15)",
          "min(0.1)",
          "min(c3)",
          "min(c4)",
          "min(c5)",
          "max(15)",
          "max(0.1)",
          "max(c3)",
          "max(c4)",
          "max(c5)"};
    }
    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {0, 1, 6},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();

    assertQueryInTestMultiKey(op, ignoreNullKeys, distinct);

    std::shared_ptr<const PlanNode> vPlan2 =
        testRoundTripPlanConvertor(op, "testveloxSubstraitRoundTripMultiKey");

    std::shared_ptr<PlanNode> op2 = std::const_pointer_cast<PlanNode>(vPlan2);
    assertQueryInTestMultiKey(op2, ignoreNullKeys, distinct);
  }

  void testPlanConvertorFromVelox(
      std::shared_ptr<core::PlanNode>& vPlan,
      std::string FunName) {
    auto message = vPlan->toString(true, true);
    std::cout << message << std::endl;
    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);
    LOG(INFO) << "Substrait Plan in  " << FunName << " is " << std::endl;
    sPlan->PrintDebugString();
  }

  std::shared_ptr<const PlanNode> testRoundTripPlanConvertor(
      std::shared_ptr<core::PlanNode>& vPlan,
      std::string FunName) {
    auto message = vPlan->toString(true, true);
    std::cout << message << std::endl;
    v2SPlanConvertor->veloxToSubstraitIR(vPlan, *sPlan);
    LOG(INFO) << "Substrait Plan in  " << FunName << " is " << std::endl;
    sPlan->PrintDebugString();

    // Convert Back
    std::shared_ptr<const PlanNode> vPlan2 =
        s2VPlanConvertor->substraitIRToVelox(*sPlan);
    auto mesage2 = vPlan2->toString(true, true);
    LOG(INFO)
        << "After transform from substrait, velox plan in assertVeloxSubstraitRoundTripFilter is :\n"
        << mesage2 << std::endl;

    return vPlan2;
  }

  void SetUp() override {
    v2SPlanConvertor = new VeloxToSubstraitPlanConvertor();
    s2VPlanConvertor = new SubstraitToVeloxPlanConvertor();
    sPlan = new io::substrait::Plan();
  }

  void TearDown() override {
    delete v2SPlanConvertor;
    delete s2VPlanConvertor;
    delete sPlan;
  }

  std::vector<RowVectorPtr>
  makeVector(int64_t size, int64_t childSize, int64_t batchSize) {
    // childSize is the size of rowType_, the number of columns
    //  size is the number of RowVectorPtr.
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

  io::substrait::Plan* sPlan;
  VeloxToSubstraitPlanConvertor* v2SPlanConvertor;
  SubstraitToVeloxPlanConvertor* s2VPlanConvertor;

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           INTEGER()})};
  folly::Random::DefaultGenerator rng_;
};

TEST_F(SubstraitAggregationTest, global) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {},
                    {"sum(15)",
                     "sum(c1)",
                     "sum(c2)",
                     "sum(c4)",
                     "sum(c5)",
                     "min(15)",
                     "min(c1)",
                     "min(c2)",
                     "min(c3)",
                     "min(c4)",
                     "min(c5)",
                     "max(15)",
                     "max(c1)",
                     "max(c2)",
                     "max(c3)",
                     "max(c4)",
                     "max(c5)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  assertQuery(
      op,
      "SELECT sum(15), sum(c1), sum(c2), sum(c4), sum(c5), min(15), min(c1), min(c2), min(c3), min(c4), min(c5), max(15), max(c1), max(c2), max(c3), max(c4), max(c5) FROM tmp");

  testPlanConvertorFromVelox(op, "global");
}

TEST_F(SubstraitAggregationTest, veloxSubstraitRoundTripGlobal) {
  auto vectors = makeVector(2, 7, 3);

  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {},
                    {"sum(15)",
                     "sum(c1)",
                     "sum(c2)",
                     "sum(c4)",
                     "sum(c5)",
                     "min(15)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  assertQuery(
      op,
      "SELECT sum(15), sum(c1), sum(c2), sum(c4), sum(c5), min(15) FROM tmp");

  // convert back
  std::shared_ptr<const PlanNode> vPlan2 =
      testRoundTripPlanConvertor(op, "veloxSubstraitRoundTripGlobal");
  assertQuery(
      vPlan2,
      "SELECT sum(15), sum(c1), sum(c2), sum(c4), sum(c5), min(15) FROM tmp");
}

TEST_F(SubstraitAggregationTest, VeloxToSubstraitSingleBigintKey) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testVeloxToSubstraitSingleKey<int64_t>(
      std::move(vectors), "c0", false, false);
  testVeloxToSubstraitSingleKey<int64_t>(std::move(vectors), "c0", true, false);
}

TEST_F(SubstraitAggregationTest, veloxSubstraitRoundTripSingleBigintKey) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testveloxSubstraitRoundTripSingleKey<int64_t>(
      std::move(vectors), "c0", false, false);
  testveloxSubstraitRoundTripSingleKey<int64_t>(
      std::move(vectors), "c0", true, false);
}

TEST_F(SubstraitAggregationTest, VeloxToSubstraitSingleBigintKeyDistinct) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testVeloxToSubstraitSingleKey<int64_t>(vectors, "c0", false, true);
  testVeloxToSubstraitSingleKey<int64_t>(vectors, "c0", true, true);
}

TEST_F(
    SubstraitAggregationTest,
    veloxSubstraitRoundTripSingleBigintKeyDistinct) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testveloxSubstraitRoundTripSingleKey<int64_t>(
      std::move(vectors), "c0", false, true);
  testveloxSubstraitRoundTripSingleKey<int64_t>(
      std::move(vectors), "c0", true, true);
}

TEST_F(SubstraitAggregationTest, veloxToSubstraitSingleStringKey) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testVeloxToSubstraitSingleKey<StringView>(vectors, "c6", false, false);
  testVeloxToSubstraitSingleKey<StringView>(vectors, "c6", true, false);
}

TEST_F(SubstraitAggregationTest, veloxToSubstraitSingleStringKeyDistinct) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testVeloxToSubstraitSingleKey<StringView>(vectors, "c6", false, true);
  testVeloxToSubstraitSingleKey<StringView>(vectors, "c6", true, true);
}

TEST_F(SubstraitAggregationTest, veloxToSubstraitMultiKey) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testVeloxToSubstraitMultiKey(vectors, false, false);
  testVeloxToSubstraitMultiKey(vectors, true, false);
}

TEST_F(SubstraitAggregationTest, veloxToSubstraitMultiKeyDistinct) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testVeloxToSubstraitMultiKey(vectors, false, true);
  testVeloxToSubstraitMultiKey(vectors, true, true);
}

TEST_F(SubstraitAggregationTest, veloxSubstraitRoundTripMultiKey) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testVeloxSubstraitRoundTripMultiKey(vectors, false, false);
  testVeloxSubstraitRoundTripMultiKey(vectors, true, false);
}

TEST_F(SubstraitAggregationTest, veloxSubstraitRoundTripMultiKeyDistinct) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testVeloxSubstraitRoundTripMultiKey(vectors, false, true);
  testVeloxSubstraitRoundTripMultiKey(vectors, true, true);
}

TEST_F(SubstraitAggregationTest, veloxToSubstraitAggregateOfNulls) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), INTEGER()});

  auto children = {
      BatchMaker::createVector<TypeKind::INTEGER>(
          rowType_->childAt(0), 3, *pool_),
      BaseVector::createConstant(
          facebook::velox::variant(TypeKind::INTEGER), 3, pool_.get()),
  };

  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, BufferPtr(nullptr), 3, children);
  auto vectors = {rowVector};
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .aggregation(
                       {0},
                       {"sum(c1)", "min(c1)", "max(c1)"},
                       {},
                       core::AggregationNode::Step::kPartial,
                       false)
                   .planNode();

  assertQuery(
      vPlan, "SELECT c0, sum(c1), min(c1), max(c1) FROM tmp GROUP BY c0");

  testPlanConvertorFromVelox(vPlan, "aggregateOfNulls");

  // global aggregation
  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {},
                    {"sum(c1)", "min(c1)", "max(c1)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();
  assertQuery(op, "SELECT sum(c1), min(c1), max(c1) FROM tmp");
  testPlanConvertorFromVelox(vPlan, "aggregateOfNulls without groupby");
}

TEST_F(SubstraitAggregationTest, veloxSubstraitRoundTripAggregateOfNulls) {
  std::vector<RowVectorPtr> vectors;
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), INTEGER()});

  auto children = {
      BatchMaker::createVector<TypeKind::INTEGER>(
          rowType_->childAt(0), 3, *pool_),
      BaseVector::createConstant(
          facebook::velox::variant(TypeKind::INTEGER), 3, pool_.get()),
  };

  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, BufferPtr(nullptr), 3, children);

  auto rowVector1 = rowVector;
  vectors.emplace_back(rowVector);
  vectors.emplace_back(rowVector1);

  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .aggregation(
                       {0},
                       {"sum(c1)", "min(c1)", "max(c1)"},
                       {},
                       core::AggregationNode::Step::kPartial,
                       false)
                   .planNode();

  assertQuery(
      vPlan, "SELECT c0, sum(c1), min(c1), max(c1) FROM tmp GROUP BY c0");

  // convert back
  std::shared_ptr<const PlanNode> vPlan2 = testRoundTripPlanConvertor(
      vPlan, "veloxSubstraitRoundTripAggregateOfNulls");
  assertQuery(
      vPlan2, "SELECT c0, sum(c1), min(c1), max(c1) FROM tmp GROUP BY c0");
}

TEST_F(
    SubstraitAggregationTest,
    veloxSubstraitRoundTripAggregateOfNullsWoGroupBy) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), INTEGER()});

  auto children = {
      BatchMaker::createVector<TypeKind::INTEGER>(
          rowType_->childAt(0), 3, *pool_),
      BaseVector::createConstant(
          facebook::velox::variant(TypeKind::INTEGER), 3, pool_.get()),
  };

  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, BufferPtr(nullptr), 3, children);
  auto vectors = {rowVector};
  createDuckDbTable(vectors);

  // global aggregation
  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {},
                    {"sum(c1)", "min(c1)", "max(c1)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  assertQuery(op, "SELECT sum(c1), min(c1), max(c1) FROM tmp");

  // convert back
  std::shared_ptr<const PlanNode> vPlan3 = testRoundTripPlanConvertor(
      op, "veloxSubstraitRoundTripAggregateOfNullsWoGroupBy");
  assertQuery(vPlan3, "SELECT sum(c1), min(c1), max(c1) FROM tmp");
}

} // namespace
} // namespace facebook::velox::exec::test
