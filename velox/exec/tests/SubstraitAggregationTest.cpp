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

#include "velox/exec/SubstraitIRConverter.h"

using namespace facebook::velox::aggregate;
using namespace facebook::velox::aggregate::test;

using facebook::velox::test::BatchMaker;

namespace facebook::velox::exec::test {
namespace {

class SubstraitAggregationTest : public AggregationTestBase {
 protected:
  template <typename T>
  void testSingleKey(
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

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE " + keyName + " IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct " + keyName + " " + fromClause);
    } else {
      assertQuery(
          op,
          "SELECT " + keyName +
              ", sum(15), sum(cast(0.1 as double)), sum(c1), sum(c2), sum(c4), sum(c5) , min(15), min(0.1), min(c1), min(c2), min(c3), min(c4), min(c5), max(15), max(0.1), max(c1), max(c2), max(c3), max(c4), max(c5) " +
              fromClause + " GROUP BY " + keyName);
    }

    auto message = op->toString(true, true);
    std::cout << message << std::endl;
    sIRConver->toSubstraitIR(op, *sPlan);
    LOG(INFO) << "Substrait Plan in testSingleKey is " << std::endl;
    sPlan->PrintDebugString();
  }

  void testMultiKey(
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

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause +=
          " WHERE c0 IS NOT NULL AND c1 IS NOT NULL AND c6 IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct c0, c1, c6 " + fromClause);
    } else {
      assertQuery(
          op,
          "SELECT c0, c1, c6, sum(15), sum(cast(0.1 as double)), sum(c4), sum(c5), min(15), min(0.1), min(c3), min(c4), min(c5), max(15), max(0.1), max(c3), max(c4), max(c5) " +
              fromClause + " GROUP BY c0, c1, c6");
    }

    // transform to substrait plan
    auto message = op->toString(true, true);
    std::cout << message << std::endl;
    sIRConver->toSubstraitIR(op, *sPlan);
    LOG(INFO) << "Substrait Plan in testMultiKey is " << std::endl;
    sPlan->PrintDebugString();
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR()})};
  folly::Random::DefaultGenerator rng_;

  SubstraitVeloxConvertor* sIRConver = new SubstraitVeloxConvertor();
  io::substrait::Plan* sPlan = new io::substrait::Plan();
};

TEST_F(SubstraitAggregationTest, global) {
  auto vectors = makeVectors(rowType_, 2, 3);
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
}

TEST_F(SubstraitAggregationTest, singleBigintKey) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(std::move(vectors), "c0", false, false);
  testSingleKey<int64_t>(std::move(vectors), "c0", true, false);
}

TEST_F(SubstraitAggregationTest, singleBigintKeyDistinct) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(vectors, "c0", false, true);
  testSingleKey<int64_t>(vectors, "c0", true, true);
}

TEST_F(SubstraitAggregationTest, singleStringKey) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, false);
  testSingleKey<StringView>(vectors, "c6", true, false);
}
TEST_F(SubstraitAggregationTest, singleStringKeyDistinct) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, true);
  testSingleKey<StringView>(vectors, "c6", true, true);
}

TEST_F(SubstraitAggregationTest, multiKey) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testMultiKey(vectors, false, false);
  testMultiKey(vectors, true, false);
}

TEST_F(SubstraitAggregationTest, multiKeyDistinct) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testMultiKey(vectors, false, true);
  testMultiKey(vectors, true, true);
}

TEST_F(SubstraitAggregationTest, aggregateOfNulls) {
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

  auto message = vPlan->toString(true, true);
  LOG(INFO) << message << std::endl;
  sIRConver->toSubstraitIR(vPlan, *sPlan);
  LOG(INFO) << "Substrait Plan in aggregateOfNulls  is " << std::endl;
  sPlan->PrintDebugString();

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
  auto message1 = op->toString(true, true);
  LOG(INFO) << message1 << std::endl;
  sIRConver->toSubstraitIR(op, *sPlan);
  LOG(INFO) << "Substrait Plan in aggregateOfNulls without groupingKey is "
            << std::endl;
  sPlan->PrintDebugString();
}

} // namespace
} // namespace facebook::velox::exec::test
