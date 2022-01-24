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
  void testPlanConvertorFromVelox(
      std::shared_ptr<core::PlanNode>& vPlan,
      std::string FunName) {
    auto message = vPlan->toString(true, true);
    std::cout << message << std::endl;
    sIRConvertor->toSubstraitIR(vPlan, *sPlan);
    std::cout << "Substrait Plan in  " << FunName << " is ==================== "<<std::endl;
    sPlan->PrintDebugString();
  }

  std::shared_ptr<const PlanNode> testRoundTripPlanConvertor(std::shared_ptr<core::PlanNode>& vPlan, std::string FunName) {
       auto message = vPlan->toString(true, true);
        std::cout << message << std::endl;
        sIRConvertor->toSubstraitIR(vPlan, *sPlan);
        std::cout << "Substrait Plan in  "<<FunName  <<" is =====================" << std::endl;
        sPlan->PrintDebugString();

            //Convert Back
            std::shared_ptr<const PlanNode> vPlan2 = sIRConvertor->fromSubstraitIR(*sPlan);
        auto mesage2 = vPlan2->toString(true, true);
        std::cout
                    << "After transform from substrait, velox plan in  "<<FunName  <<" is ======================:\n"
                    << mesage2 << std::endl;

            return vPlan2;
      }

        void SetUp() override{
        sIRConvertor = new SubstraitVeloxConvertor();
        sPlan = new io::substrait::Plan();
      }

        void TearDown() override{
        delete sIRConvertor;
        delete sPlan;
     }

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



    testPlanConvertorFromVelox(op, "testSingleKey");

/*    // readback
    auto vPlan2 = sIRConver->fromSubstraitIR(*sPlan);

    auto mesage2 = vPlan2->toString(true, true);
    std::cout << "vPlan2 in testSingleKey trans from substrait is================\n" << mesage2 << std::endl;

    io::substrait::Plan *sPlan2 = new io::substrait::Plan();
    sIRConver->toSubstraitIR(vPlan2, *sPlan2);
    std::cout << "sPlan2 in testSingleKey is ===============" << std::endl;
    sPlan2->PrintDebugString();*/

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

    //transform to substrait plan
    testPlanConvertorFromVelox(op, "testMultiKey");
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
           VARCHAR()})};
  folly::Random::DefaultGenerator rng_;


  SubstraitVeloxConvertor *sIRConvertor ;
  io::substrait::Plan *sPlan ;
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
  testPlanConvertorFromVelox(op, "global");
}

TEST_F(SubstraitAggregationTest, singleBigintKey) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(std::move(vectors), "c0", false, false);
  //testSingleKey<int64_t>(std::move(vectors), "c0", true, false);
}

TEST_F(SubstraitAggregationTest, singleBigintKeyDistinct) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(vectors, "c0", false, true);
  //testSingleKey<int64_t>(vectors, "c0", true, true);
}

/*TEST_F(SubstraitAggregationTest, singleStringKey) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, false);
  //testSingleKey<StringView>(vectors, "c6", true, false);
}

TEST_F(SubstraitAggregationTest, singleStringKeyDistinct) {
  auto vectors = makeVectors(rowType_, 2, 3);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, true);
  //testSingleKey<StringView>(vectors, "c6", true, true);
}*/

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

  assertQuery(vPlan, "SELECT c0, sum(c1), min(c1), max(c1) FROM tmp GROUP BY c0");

  testPlanConvertorFromVelox(vPlan, "aggregateOfNulls");

/*      // readback
    auto vPlan2 = sIRConver->fromSubstraitIR(*sPlan);

  auto mesage2 = vPlan2->toString(true, true);
  std::cout << "vPlan2 in aggregateOfNulls trans from substrait is================\n" << mesage2 << std::endl;

  assertQuery(vPlan2, "SELECT c0, sum(c1), min(c1), max(c1) FROM tmp GROUP BY c0");

  auto message3 = vPlan2->toString(true, true);
  std::cout << "vPlan2  after trans from substrait and assertQuery  is================\n" << message3 << std::endl;*/

/*  io::substrait::Plan *sPlan2 = new io::substrait::Plan();
  sIRConver->toSubstraitIR(vPlan2, *sPlan2);
  std::cout << "sPlan2 in assertValues is ===============" << std::endl;
  sPlan2->PrintDebugString();*/

/*
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
  testPlanConvertorFromVelox(vPlan, "aggregateOfNulls without groupby");*/

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
