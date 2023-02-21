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

#include <folly/init/Init.h>
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

class VeloxSubstraitJoinRoundTripTest : public OperatorTestBase {
 protected:
  static std::vector<std::string> makeKeyNames(
      int cnt,
      const std::string& prefix) {
    std::vector<std::string> names;
    for (int i = 0; i < cnt; ++i) {
      names.push_back(fmt::format("{}k{}", prefix, i));
    }
    return names;
  }

  static RowTypePtr makeRowType(
      const std::vector<TypePtr>& keyTypes,
      const std::string& namePrefix) {
    std::vector<std::string> names = makeKeyNames(keyTypes.size(), namePrefix);
    names.push_back(fmt::format("{}data", namePrefix));

    std::vector<TypePtr> types = keyTypes;
    types.push_back(VARCHAR());

    return ROW(std::move(names), std::move(types));
  }

  static std::vector<std::string> concat(
      const std::vector<std::string>& a,
      const std::vector<std::string>& b) {
    std::vector<std::string> result;
    result.insert(result.end(), a.begin(), a.end());
    result.insert(result.end(), b.begin(), b.end());
    return result;
  }

  static CursorParameters makeCursorParameters(
      const std::shared_ptr<const core::PlanNode>& planNode,
      uint32_t preferredOutputBatchSize) {
    auto queryCtx = core::QueryCtx::createForTest();
    queryCtx->setConfigOverridesUnsafe(
        {{core::QueryConfig::kCreateEmptyFiles, "true"}});

    CursorParameters params;
    params.planNode = planNode;
    params.queryCtx = core::QueryCtx::createForTest();
    params.queryCtx->setConfigOverridesUnsafe(
        {{core::QueryConfig::kPreferredOutputBatchSize,
          std::to_string(preferredOutputBatchSize)}});
    return params;
  }

  template <typename T>
  VectorPtr sequence(vector_size_t size, T start = 0) {
    return makeFlatVector<int32_t>(
        size, [start](auto row) { return start + row; });
  }

  void testJoin(
      const std::vector<TypePtr>& keyTypes,
      int32_t leftSize,
      int32_t rightSize,
      const std::string& referenceQuery,
      const std::string& filter = "",
      const std::vector<std::string>& outputLayout = {},
      core::JoinType joinType = core::JoinType::kInner) {
    auto leftType = makeRowType(keyTypes, "t_");
    auto rightType = makeRowType(keyTypes, "u_");

    auto leftBatch = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(leftType, leftSize, *pool_));
    auto rightBatch = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rightType, rightSize, *pool_));

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    auto planNode = PlanBuilder(planNodeIdGenerator)
                        .values({leftBatch})
                        .hashJoin(
                            makeKeyNames(keyTypes.size(), "t_"),
                            makeKeyNames(keyTypes.size(), "u_"),
                            PlanBuilder(planNodeIdGenerator)
                                .values({rightBatch})
                                .planNode(),
                            filter,
                            outputLayout.empty()
                                ? concat(leftType->names(), rightType->names())
                                : outputLayout)
                        .planNode();

    createDuckDbTable("t", {leftBatch});
    createDuckDbTable("u", {rightBatch});
    assertQuery(planNode, referenceQuery);
    assertPlanConversion(planNode, referenceQuery);
  }

  template <typename T>
  void testMergeJoin(
      std::function<T(vector_size_t /*row*/)> leftKeyAt,
      std::function<T(vector_size_t /*row*/)> rightKeyAt) {
    // Single batch on the left and right sides of the join.
    {
      auto leftKeys = makeFlatVector<T>(1'234, leftKeyAt);
      auto rightKeys = makeFlatVector<T>(1'234, rightKeyAt);

      testMergeJoin({leftKeys}, {rightKeys});
    }

    // Multiple batches on one side. Single batch on the other side.
    {
      std::vector<VectorPtr> leftKeys = {
          makeFlatVector<T>(1024, leftKeyAt),
          makeFlatVector<T>(
              1024, [&](auto row) { return leftKeyAt(1024 + row); }),
      };
      std::vector<VectorPtr> rightKeys = {makeFlatVector<T>(2048, rightKeyAt)};

      testMergeJoin(leftKeys, rightKeys);

      // Swap left and right side keys.
      testMergeJoin(rightKeys, leftKeys);
    }

    // Multiple batches on each side.
    {
      std::vector<VectorPtr> leftKeys = {
          makeFlatVector<T>(512, leftKeyAt),
          makeFlatVector<T>(
              1024, [&](auto row) { return leftKeyAt(512 + row); }),
          makeFlatVector<T>(
              16, [&](auto row) { return leftKeyAt(512 + 1024 + row); }),
      };
      std::vector<VectorPtr> rightKeys = {
          makeFlatVector<T>(123, rightKeyAt),
          makeFlatVector<T>(
              1024, [&](auto row) { return rightKeyAt(123 + row); }),
          makeFlatVector<T>(
              1234, [&](auto row) { return rightKeyAt(123 + 1024 + row); }),
      };

      testMergeJoin(leftKeys, rightKeys);

      // Swap left and right side keys.
      testMergeJoin(rightKeys, leftKeys);
    }
  }

  void testMergeJoin(
      const std::vector<VectorPtr>& leftKeys,
      const std::vector<VectorPtr>& rightKeys) {
    std::vector<RowVectorPtr> left;
    left.reserve(leftKeys.size());
    vector_size_t startRow = 0;
    for (const auto& key : leftKeys) {
      auto payload = makeFlatVector<int32_t>(
          key->size(), [startRow](auto row) { return (startRow + row) * 10; });
      left.push_back(makeRowVector({key, payload}));
      startRow += key->size();
    }

    std::vector<RowVectorPtr> right;
    right.reserve(rightKeys.size());
    startRow = 0;
    for (const auto& key : rightKeys) {
      auto payload = makeFlatVector<int32_t>(
          key->size(), [startRow](auto row) { return (startRow + row) * 20; });
      right.push_back(makeRowVector({key, payload}));
      startRow += key->size();
    }

    createDuckDbTable("t", left);
    createDuckDbTable("u", right);

    // Test INNER join.
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(left)
                    .mergeJoin(
                        {"c0"},
                        {"u_c0"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(right)
                            .project({"c1 AS u_c1", "c0 AS u_c0"})
                            .planNode(),
                        "",
                        {"c0", "c1", "u_c1"},
                        core::JoinType::kInner)
                    .planNode();
    std::string duckDbSql =
        "SELECT t.c0, t.c1, u.c1 FROM t, u WHERE t.c0 = u.c0";
    // Use very small output batch size.
    assertPlanConversion(plan, 16, duckDbSql);
    assertQuery(
        makeCursorParameters(plan, 16),
        "SELECT t.c0, t.c1, u.c1 FROM t, u WHERE t.c0 = u.c0");

    // Use regular output batch size.
    assertQuery(
        makeCursorParameters(plan, 1024),
        "SELECT t.c0, t.c1, u.c1 FROM t, u WHERE t.c0 = u.c0");
    assertPlanConversion(plan, 1024, duckDbSql);
    // Use very large output batch size.
    assertQuery(
        makeCursorParameters(plan, 10'000),
        "SELECT t.c0, t.c1, u.c1 FROM t, u WHERE t.c0 = u.c0");
    assertPlanConversion(plan, 10000, duckDbSql);
    // Test LEFT join.
    planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    plan = PlanBuilder(planNodeIdGenerator)
               .values(left)
               .mergeJoin(
                   {"c0"},
                   {"u_c0"},
                   PlanBuilder(planNodeIdGenerator)
                       .values(right)
                       .project({"c1 as u_c1", "c0 as u_c0"})
                       .planNode(),
                   "",
                   {"c0", "c1", "u_c1"},
                   core::JoinType::kLeft)
               .planNode();
    duckDbSql = "SELECT t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0";
    // Use very small output batch size.
    assertQuery(
        makeCursorParameters(plan, 16),
        "SELECT t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0");
    assertPlanConversion(plan, 16, duckDbSql);
    // Use regular output batch size.
    assertQuery(
        makeCursorParameters(plan, 1024),
        "SELECT t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0");
    assertPlanConversion(plan, 1024, duckDbSql);
    // Use very large output batch size.
    assertQuery(
        makeCursorParameters(plan, 10'000),
        "SELECT t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0");
    assertPlanConversion(plan, 10000, duckDbSql);
  }

  void assertPlanConversion(
      const std::shared_ptr<const core::PlanNode>& plan,
      uint32_t preferredOutputBatchSize,
      const std::string& duckDbSql) {
    assertQuery(
        makeCursorParameters(plan, preferredOutputBatchSize), duckDbSql);

    // Convert Velox Plan to Substrait Plan.
    google::protobuf::Arena arena;
    auto substraitPlan = veloxConvertor_->toSubstrait(arena, plan);

    // Convert Substrait Plan to the same Velox Plan.
    auto samePlan = substraitConverter_->toVeloxPlan(substraitPlan);
    std::cout << "The smae plan is : \n"
              << samePlan->toString(true, true) << std::endl;
    // Assert velox again.
    // assertQuery(samePlan, duckDbSql);
    assertQuery(
        makeCursorParameters(samePlan, preferredOutputBatchSize), duckDbSql);
  }

  void assertPlanConversion(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::string& duckDbSql) {
    assertQuery(plan, duckDbSql);

    // Convert Velox Plan to Substrait Plan.
    google::protobuf::Arena arena;
    auto substraitPlan = veloxConvertor_->toSubstrait(arena, plan);

    // Convert Substrait Plan to the same Velox Plan.
    auto samePlan = substraitConverter_->toVeloxPlan(substraitPlan);
    std::cout << "The smae plan is : \n"
              << samePlan->toString(true, true) << std::endl;
    // Assert velox again.
    assertQuery(samePlan, duckDbSql);
  }

  std::shared_ptr<VeloxToSubstraitPlanConvertor> veloxConvertor_ =
      std::make_shared<VeloxToSubstraitPlanConvertor>();

  std::shared_ptr<SubstraitVeloxPlanConverter> substraitConverter_ =
      std::make_shared<SubstraitVeloxPlanConverter>(pool_.get());
};

TEST_F(VeloxSubstraitJoinRoundTripTest, bigintArray) {
  testJoin(
      {BIGINT()},
      16000,
      15000,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_F(VeloxSubstraitJoinRoundTripTest, emptyBuild) {
  testJoin(
      {BIGINT()},
      16000,
      0,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_F(VeloxSubstraitJoinRoundTripTest, normalizedKey) {
  std::vector<std::string> output = {
      "t_k0", "t_k1", "t_data", "u_k0", "u_k1", "u_data"};
  testJoin(
      {INTEGER(), INTEGER(), INTEGER()},
      16000,
      1500,
      "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1",
      "",
      output);
}

TEST_F(VeloxSubstraitJoinRoundTripTest, filter) {
  testJoin(
      {BIGINT()},
      16000,
      15000,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND ((t_k0 % 100) + (u_k0 % 100)) % 40 < 20",
      "((t_k0 % 100) + (u_k0 % 100)) % 40 < 20");
}

TEST_F(VeloxSubstraitJoinRoundTripTest, leftJoin) {
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3}),
          makeNullableFlatVector<int32_t>({10, std::nullopt, 30}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3}),
          makeNullableFlatVector<int32_t>({std::nullopt, 20, 30}),
      })};
  auto rightVectors = {
      makeRowVector({makeFlatVector<int32_t>({1, 2, 10})}),
  };

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", rightVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values(rightVectors)
                       .project({"c0 AS u_c0"})
                       .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(leftVectors)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "c1 + u_c0 > 0",
                      {"c0", "u_c0"},
                      core::JoinType::kLeft)
                  .planNode();

  assertPlanConversion(
      plan,
      "SELECT t.c0,u.c0  FROM t LEFT JOIN u ON (t.c0 = u.c0 AND t.c1 + u.c0 > 0)");
}

TEST_F(VeloxSubstraitJoinRoundTripTest, rightJoin) {
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeNullableFlatVector<int32_t>(
              {10, std::nullopt, 30, std::nullopt, 50}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeNullableFlatVector<int32_t>(
              {std::nullopt, 20, 30, std::nullopt, 50}),
      })};
  auto rightVectors = {
      makeRowVector({makeFlatVector<int32_t>({1, 2, 10, 30, 40})}),
  };

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", rightVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values(rightVectors)
                       .project({"c0 AS u_c0"})
                       .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(leftVectors)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "c1 + u_c0 > 0",
                      {"c0", "c1"},
                      core::JoinType::kRight)
                  .planNode();

  assertPlanConversion(
      plan,
      "SELECT t.c0, t.c1 FROM t RIGHT JOIN u ON (t.c0 = u.c0 AND t.c1 + u.c0 > 0)");
}

TEST_F(VeloxSubstraitJoinRoundTripTest, leftSemiJoin) {
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'234, [](auto row) { return row % 11; }, nullEvery(13)),
      makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
  });

  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return row % 5; }, nullEvery(7)),
  });

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .project({"c0 as u_c0"})
                        .planNode(),
                    "",
                    {"c1"},
                    core::JoinType::kLeftSemi)
                .planNode();

  assertPlanConversion(
      op, "SELECT t.c1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)");
}

TEST_F(VeloxSubstraitJoinRoundTripTest, fullJoin) {
  // Left side keys are [0, 1, 2,..10].
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>(
              2'222, [](auto row) { return row % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(2'222, [](auto row) { return row; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(
              2'222, [](auto row) { return (row + 3) % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(2'222, [](auto row) { return row; }),
      }),
  };

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
      makeFlatVector<int32_t>(
          123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
  });

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values({rightVectors})
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();

  auto op = PlanBuilder(planNodeIdGenerator)
                .values(leftVectors)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide,
                    "",
                    {"c0", "c1", "u_c1"},
                    core::JoinType::kFull)
                .planNode();

  assertPlanConversion(
      op, "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0");
}

TEST_F(VeloxSubstraitJoinRoundTripTest, antiJoin) {
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'000, [](auto row) { return row % 11; }, nullEvery(13)),
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
  });

  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'234, [](auto row) { return row % 5; }, nullEvery(7)),
  });

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"c0"},
                    {"c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .filter("c0 IS NOT NULL")
                        .planNode(),
                    "",
                    {"c1"},
                    core::JoinType::kNullAwareAnti)
                .planNode();

  assertQuery(
      op,
      "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 IS NOT NULL)");
}

TEST_F(VeloxSubstraitJoinRoundTripTest, MergeJoinOneToOneAllMatch) {
  testMergeJoin<int32_t>(
      [](auto row) { return row; }, [](auto row) { return row; });
}

TEST_F(VeloxSubstraitJoinRoundTripTest, allRowsMatch) {
  std::vector<VectorPtr> leftKeys = {
      makeFlatVector<int32_t>(2, [](auto /* row */) { return 5; }),
      makeFlatVector<int32_t>(3, [](auto /* row */) { return 5; }),
      makeFlatVector<int32_t>(4, [](auto /* row */) { return 5; }),
  };
  std::vector<VectorPtr> rightKeys = {
      makeFlatVector<int32_t>(7, [](auto /* row */) { return 5; })};

  testMergeJoin(leftKeys, rightKeys);

  testMergeJoin(rightKeys, leftKeys);
}

TEST_F(VeloxSubstraitJoinRoundTripTest, nonFirstMergeJoinKeys) {
  auto left = makeRowVector(
      {"t_data", "t_key"},
      {
          makeFlatVector<int32_t>({50, 40, 30, 20, 10}),
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
      });
  auto right = makeRowVector(
      {"u_data", "u_key"},
      {
          makeFlatVector<int32_t>({23, 22, 21}),
          makeFlatVector<int32_t>({2, 4, 6}),
      });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({left})
          .mergeJoin(
              {"t_key"},
              {"u_key"},
              PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
              "",
              {"t_key", "t_data", "u_data"},
              core::JoinType::kInner)
          .planNode();

  assertQuery(plan, "VALUES (2, 40, 23), (4, 20, 22)");
  assertPlanConversion(plan, "VALUES (2, 40, 23), (4, 20, 22)");
}

//TEST_F(VeloxSubstraitJoinRoundTripTest, crossJoinbasic) {
//  auto leftVectors = {
//      makeRowVector({sequence<int32_t>(10)}),
//      makeRowVector({sequence<int32_t>(100, 10)}),
//      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
//      makeRowVector({sequence<int32_t>(7, 10 + 100 + 1'000)}),
//  };
//
//  auto rightVectors = {
//      makeRowVector({sequence<int32_t>(10)}),
//      makeRowVector({sequence<int32_t>(100, 10)}),
//      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
//      makeRowVector({sequence<int32_t>(11, 10 + 100 + 1'000)}),
//  };
//  auto leftVectors = {
//      makeRowVector({
//          makeFlatVector<int32_t>({1, 2, 3}),
//      }),
//      makeRowVector({
//          makeFlatVector<int32_t>({1, 2, 3}),
//      })};
//  auto rightVectors = {
//      makeRowVector({makeFlatVector<int32_t>({1, 2, 10})}),
//  };

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", rightVectors);

  // All x 13. Join output vectors contains multiple probe rows each.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values(leftVectors)
                .crossJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values(rightVectors)
                        .filter("c0 < 13")
                        .project({"c0 AS u_c0"})
                        .planNode(),
                    {"c0", "u_c0"})
                .planNode();

  assertPlanConversion(op, "SELECT * FROM t, u WHERE u.c0 < 13");

//  // 13 x all. Join output vectors contains single probe row each.
//  planNodeIdGenerator->reset();
//  op = PlanBuilder(planNodeIdGenerator)
//           .values({leftVectors})
//           .filter("c0 < 13")
//           .crossJoin(
//               PlanBuilder(planNodeIdGenerator)
//                   .values({rightVectors})
//                   .project({"c0 AS u_c0"})
//                   .planNode(),
//               {"c0", "u_c0"})
//           .planNode();
//
//  assertPlanConversion(op, "SELECT * FROM t, u WHERE t.c0 < 13");

//  // All x 13. No columns on the build side.
//  planNodeIdGenerator->reset();
//  op = PlanBuilder(planNodeIdGenerator)
//           .values({leftVectors})
//           .crossJoin(
//               PlanBuilder(planNodeIdGenerator)
//                   .values({vectorMaker_.rowVector(ROW({}, {}), 13)})
//                   .planNode(),
//               {"c0"})
//           .planNode();
//
//  assertPlanConversion(op, "SELECT t.* FROM t, (SELECT * FROM u LIMIT 13) u");
//
//  // 13 x All. No columns on the build side.
//  planNodeIdGenerator->reset();
//  op = PlanBuilder(planNodeIdGenerator)
//           .values({leftVectors})
//           .filter("c0 < 13")
//           .crossJoin(
//               PlanBuilder(planNodeIdGenerator)
//                   .values({vectorMaker_.rowVector(ROW({}, {}), 1121)})
//                   .planNode(),
//               {"c0"})
//           .planNode();
//
//  assertPlanConversion(
//      op,
//      "SELECT t.* FROM (SELECT * FROM t WHERE c0 < 13) t, (SELECT * FROM u LIMIT 1121) u");
//
//  // Empty build side.
//  planNodeIdGenerator->reset();
//  op = PlanBuilder(planNodeIdGenerator)
//           .values({leftVectors})
//           .crossJoin(
//               PlanBuilder(planNodeIdGenerator)
//                   .values({rightVectors})
//                   .filter("c0 < 0")
//                   .project({"c0 AS u_c0"})
//                   .planNode(),
//               {"c0", "u_c0"})
//           .planNode();
//
//  assertQueryReturnsEmptyResult(op);
//
//  // Multi-threaded build side.
//  planNodeIdGenerator->reset();
//  CursorParameters params;
//  params.maxDrivers = 4;
//  params.planNode = PlanBuilder(planNodeIdGenerator)
//                        .values({leftVectors})
//                        .crossJoin(
//                            PlanBuilder(planNodeIdGenerator, pool_.get())
//                                .values({rightVectors}, true)
//                                .filter("c0 in (10, 17)")
//                                .project({"c0 AS u_c0"})
//                                .planNode(),
//                            {"c0", "u_c0"})
//                        .limit(0, 100'000, false)
//                        .planNode();
//
//  OperatorTestBase::assertQuery(
//      params,
//      "SELECT * FROM t, (SELECT * FROM UNNEST (ARRAY[10, 17, 10, 17, 10, 17, 10, 17])) u");
}

TEST_F(VeloxSubstraitJoinRoundTripTest, leftMergeJoin) {
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3}),
          makeNullableFlatVector<int32_t>({10, std::nullopt, 30}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3}),
          makeNullableFlatVector<int32_t>({std::nullopt, 20, 30}),
      })};
  auto rightVectors = {
      makeRowVector({makeFlatVector<int32_t>({1, 2, 10})}),
  };

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", rightVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values(rightVectors)
                       .project({"c0 AS u_c0"})
                       .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(leftVectors)
                  .mergeJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "c1 + u_c0 > 0",
                      {"c0", "u_c0"},
                      core::JoinType::kLeft)
                  .planNode();

  assertPlanConversion(
      plan,
      "SELECT t.c0,u.c0  FROM t LEFT JOIN u ON (t.c0 = u.c0 AND t.c1 + u.c0 > 0)");
}