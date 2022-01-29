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

  void testPlanConvertorFromVelox(
      std::shared_ptr<core::PlanNode>& vPlan,
      std::string FunName) {
    auto message = vPlan->toString(true, true);
    std::cout << message << std::endl;
    sIRConvertor->toSubstraitIR(vPlan, *sPlan);
    std::cout << "Substrait Plan in  " << FunName
              << " is ==================== " << std::endl;
    sPlan->PrintDebugString();
  }

  std::shared_ptr<const PlanNode> testRoundTripPlanConvertor(
      std::shared_ptr<PlanNode>& vPlan,
      std::string FunName) {
    auto message = vPlan->toString(true, true);
    std::cout << message << std::endl;
    sIRConvertor->toSubstraitIR(vPlan, *sPlan);
    std::cout << "Substrait Plan in  " << FunName
              << " is =====================" << std::endl;
    sPlan->PrintDebugString();

    // Convert Back
    std::shared_ptr<const PlanNode> vPlan2 =
        sIRConvertor->fromSubstraitIR(*sPlan);
    auto mesage2 = vPlan2->toString(true, true);
    std::cout << "After transform from substrait, velox plan in  " << FunName
              << " is ======================:\n"
              << mesage2 << std::endl;

    return vPlan2;
  }

  void SetUp() override {
    sIRConvertor = new SubstraitVeloxConvertor();
    sPlan = new io::substrait::Plan();
  }

  void TearDown() override {
    delete sIRConvertor;
    delete sPlan;
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

  template <typename T>
  void setTestKey(
      int64_t value,
      int32_t multiplier,
      vector_size_t row,
      FlatVector<T>* vector) {
    vector->set(row, value * multiplier);
  }

  template <typename T>
  void setKey(
      int32_t column,
      int32_t cardinality,
      int32_t multiplier,
      int32_t row,
      RowVector* batch) {
    auto vector = batch->childAt(column)->asUnchecked<FlatVector<T>>();
    auto value = folly::Random::rand32(rng_) % cardinality;
    setTestKey(value, multiplier, row, vector);
  }

  void makeModeTestKeys(
      TypePtr rowType,
      int32_t numRows,
      int32_t c0,
      int32_t c1,
      int32_t c2,
      int32_t c3,
      int32_t c4,
      int32_t c5,
      std::vector<RowVectorPtr>& batches) {
    RowVectorPtr rowVector;
    for (auto count = 0; count < numRows; ++count) {
      if (count % 1000 == 0) {
        rowVector = std::static_pointer_cast<RowVector>(BaseVector::create(
            rowType, std::min(1000, numRows - count), pool_.get()));
        batches.push_back(rowVector);
        for (auto& child : rowVector->children()) {
          child->resize(1000);
        }
      }
      setKey<int64_t>(0, c0, 6, count % 1000, rowVector.get());
      setKey<int16_t>(1, c1, 1, count % 1000, rowVector.get());
      setKey<int8_t>(2, c2, 1, count % 1000, rowVector.get());
      setKey<StringView>(3, c3, 2, count % 1000, rowVector.get());
      setKey<StringView>(4, c4, 5, count % 1000, rowVector.get());
      setKey<StringView>(5, c5, 8, count % 1000, rowVector.get());
    }
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           INTEGER()})};

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

  folly::Random::DefaultGenerator rng_;

  SubstraitVeloxConvertor* sIRConvertor;
  io::substrait::Plan* sPlan;
};

template <>
void SubstraitAggregationTest::setTestKey(
    int64_t value,
    int32_t multiplier,
    vector_size_t row,
    FlatVector<StringView>* vector) {
  std::string chars;
  if (multiplier == 2) {
    chars.resize(2);
    chars[0] = (value % 64) + 32;
    chars[1] = ((value / 64) % 64) + 32;
  } else {
    chars = fmt::format("{}", value);
    for (int i = 2; i < multiplier; ++i) {
      chars = chars + fmt::format("{}", i * value);
    }
  }
  vector->set(row, StringView(chars));
}

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
 // auto vectors = makeVectors(rowType_, 1, 1);
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

  std::shared_ptr<const PlanNode> vPlan2 = testRoundTripPlanConvertor(op, "veloxSubstraitRoundTripGlobal");

  //std::shared_ptr<PlanNode> op2 = std::const_pointer_cast<PlanNode>(vPlan2);
  assertQuery(vPlan2,
      "SELECT sum(15), sum(c1), sum(c2), sum(c4), sum(c5), min(15) FROM tmp");


  auto message3 = vPlan2->toString(true, true);
  std::cout << "vPlan2  after trans from substrait and assertQuery is================\n" << message3 << std::endl;
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

TEST_F(SubstraitAggregationTest, veloxSubstraitRoundTripSingleBigintKeyDistinct) {
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

TEST_F(SubstraitAggregationTest, veloxSubstraitRoundTripSingleStringKey) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testveloxSubstraitRoundTripSingleKey<StringView>(vectors, "c6", false, false);
  testveloxSubstraitRoundTripSingleKey<StringView>(vectors, "c6", true, false);
}

TEST_F(SubstraitAggregationTest, veloxSubstraitRoundTripSingleStringKeyDistinct) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  testveloxSubstraitRoundTripSingleKey<StringView>(vectors, "c6", false, true);
  testveloxSubstraitRoundTripSingleKey<StringView>(vectors, "c6", true, true);
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
                       {"sum(c1)", "avg(c1)","min(c1)", "max(c1)"},
                       {},
                       core::AggregationNode::Step::kPartial,
                       false)
                   .planNode();

  assertQuery(
      vPlan, "SELECT c0, sum(c1), avg(c1), min(c1), max(c1) FROM tmp GROUP BY c0");

  // convert back
  std::shared_ptr<const PlanNode> vPlan2 = testRoundTripPlanConvertor(vPlan, "veloxSubstraitRoundTripAggregateOfNulls");
  assertQuery(vPlan2, "SELECT c0, sum(c1),avg(c1), min(c1), max(c1) FROM tmp GROUP BY c0");

  auto message3 = vPlan2->toString(true, true);
  std::cout << "vPlan2  after trans from substrait and assertQuery is================\n" << message3 << std::endl;
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
                    {"avg(c1)", "max(c0)", "min(c1)", "max(c1)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  assertQuery(op, "SELECT avg(c1), max(c0),min(c1), max(c1) FROM tmp");
  // convert back

  std::shared_ptr<const PlanNode> vPlan3 =
      testRoundTripPlanConvertor(op, "veloxSubstraitRoundTripAggregateOfNulls");
  assertQuery(vPlan3, "SELECT avg(c1), max(c0),min(c1), max(c1) FROM tmp");

  auto message4 = vPlan3->toString(true, true);
  std::cout
      << "vPlan2  after trans from substrait and assertQuery is================\n"
      << message4 << std::endl;
}

TEST_F(SubstraitAggregationTest, hashmodes) {
  rng_.seed(1);
  std::vector<std::string> keyNames = {"C0", "C1", "C2", "C3", "C4", "C5"};
  std::vector<std::shared_ptr<const Type>> types = {
      BIGINT(), SMALLINT(), TINYINT(), VARCHAR(), VARCHAR(), VARCHAR()};
  rowType_ = std::make_shared<RowType>(std::move(keyNames), std::move(types));

  std::vector<RowVectorPtr> batches;

  // 20K rows with all at low cardinality.
  makeModeTestKeys(rowType_, 20000, 2, 2, 2, 4, 4, 4, batches);
  // 20K rows with all at slightly higher cardinality, still in array range.
  makeModeTestKeys(rowType_, 20000, 2, 2, 2, 4, 16, 4, batches);
  // 100K rows with cardinality outside of array range. We transit to
  // generic hash table from normalized keys when running out of quota
  // for distinct string storage for the sixth key.
  makeModeTestKeys(rowType_, 100000, 1000000, 2, 2, 4, 4, 1000000, batches);
  createDuckDbTable(batches);
  auto op = PlanBuilder()
                .values(batches)
                .finalAggregation({0, 1, 2, 3, 4, 5}, {"sum(1)"})
                .planNode();

  assertQuery(
      op,
      "SELECT c0, c1, C2, C3, C4, C5, sum(1) FROM tmp "
      " GROUP BY c0, C1, C2, c3, C4, C5");
}

TEST_F(SubstraitAggregationTest, rangeToDistinct) {
  rng_.seed(1);
  std::vector<std::string> keyNames = {"C0", "C1", "C2", "C3", "C4", "C5"};
  std::vector<std::shared_ptr<const Type>> types = {
      BIGINT(), SMALLINT(), TINYINT(), VARCHAR(), VARCHAR(), VARCHAR()};
  rowType_ = std::make_shared<RowType>(std::move(keyNames), std::move(types));

  std::vector<RowVectorPtr> batches;
  // 20K rows with all at low cardinality. c0 is a range.
  makeModeTestKeys(rowType_, 20000, 2000, 2, 2, 4, 4, 4, batches);
  // 20 rows that make c0 represented as distincts.
  makeModeTestKeys(rowType_, 20, 200000000, 2, 2, 4, 4, 4, batches);
  // More keys in the low cardinality range. We see if these still hit
  // after the re-encoding of c0.
  makeModeTestKeys(rowType_, 10000, 2000, 2, 2, 4, 4, 4, batches);

  createDuckDbTable(batches);
  auto op = PlanBuilder()
                .values(batches)
                .finalAggregation({0, 1, 2, 3, 4, 5}, {"sum(1)"})
                .planNode();

  assertQuery(
      op,
      "SELECT c0, c1, C2, C3, C4, C5, sum(1) FROM tmp "
      " GROUP BY c0, C1, C2, c3, C4, C5");
}

TEST_F(SubstraitAggregationTest, allKeyTypes) {
  // Covers different key types. Unlike the integer/string tests, the
  // hash table begins life in the generic mode, not array or
  // normalized key. Add types here as they become supported.
  std::vector<std::string> keyNames = {"C0", "C1", "C2", "C3", "C4", "C5"};
  std::vector<std::shared_ptr<const Type>> types = {
      DOUBLE(), REAL(), BIGINT(), INTEGER(), BOOLEAN(), VARCHAR()};
  rowType_ = std::make_shared<RowType>(std::move(keyNames), std::move(types));

  std::vector<RowVectorPtr> batches;
  for (auto i = 0; i < 10; ++i) {
    batches.push_back(std::static_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_)));
  }
  createDuckDbTable(batches);
  auto op = PlanBuilder()
                .values(batches)
                .finalAggregation({0, 1, 2, 3, 4, 5}, {"sum(1)"})
                .planNode();

  assertQuery(
      op,
      "SELECT c0, c1, C2, C3, C4, C5, sum(1) FROM tmp "
      " GROUP BY c0, C1, C2, c3, C4, C5");
}

TEST_F(SubstraitAggregationTest, partialAggregationMemoryLimit) {
  auto vectors = {
      makeRowVector({makeFlatVector<int32_t>(
          100, [](auto row) { return row; }, nullEvery(5))}),
      makeRowVector({makeFlatVector<int32_t>(
          110, [](auto row) { return row + 29; }, nullEvery(7))}),
      makeRowVector({makeFlatVector<int32_t>(
          90, [](auto row) { return row - 71; }, nullEvery(7))}),
  };

  createDuckDbTable(vectors);

  // Set an artificially low limit on the amount of data to accumulate in
  // the partial aggregation.
  CursorParameters params;
  params.queryCtx = core::QueryCtx::create();

  params.queryCtx->setConfigOverridesUnsafe({
      {core::QueryConfig::kMaxPartialAggregationMemory, "100"},
  });

  // Distinct aggregation.
  params.planNode = PlanBuilder()
                        .values(vectors)
                        .partialAggregation({0}, {})
                        .finalAggregation({0}, {})
                        .planNode();

  assertQuery(params, "SELECT distinct c0 FROM tmp");

  // Count aggregation.
  params.planNode = PlanBuilder()
                        .values(vectors)
                        .partialAggregation({0}, {"count(1)"})
                        .finalAggregation({0}, {"sum(a0)"})
                        .planNode();

  assertQuery(params, "SELECT c0, count(1) FROM tmp GROUP BY 1");
}

} // namespace
} // namespace facebook::velox::exec::test
