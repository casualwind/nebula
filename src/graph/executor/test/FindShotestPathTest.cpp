/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>
#include <gtest/gtest.h>

#include "common/expression/VariableExpression.h"
#include "common/fs/TempDir.h"
#include "graph/context/QueryContext.h"
#include "graph/executor/algo/BFSShortestPathExecutor.h"
#include "graph/executor/logic/LoopExecutor.h"
#include "graph/executor/logic/SelectExecutor.h"
#include "graph/executor/logic/StartExecutor.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "storage/exec/EdgeNode.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace graph {
class LogicExecutorsTest : public testing::Test {
 protected:
  Path createPath(const std::vector<std::string>& steps) {
    Path path;
    path.src = Vertex(steps[0], {});
    for (size_t i = 1; i < steps.size(); ++i) {
      path.steps.emplace_back(Step(Vertex(steps[i], {}), EDGE_TYPE, "like", EDGE_RANK, {}));
    }
    return path;
  }


};

TEST_F(LogicExecutorsTest, FSP) {
  std::string counter = "counter";
  qctx_->ectx()->setValue(counter, 0);
  // ++counter{0} <= 5
  auto condition = RelationalExpression::makeLE(
      pool_,
      UnaryExpression::makeIncr(
          pool_,
          VersionedVariableExpression::make(pool_, counter, ConstantExpression::make(pool_, 0))),
      ConstantExpression::make(pool_, static_cast<int32_t>(5)));
  auto* start = StartNode::make(qctx_.get());
  auto* loop = Loop::make(qctx_.get(), start, start, condition);
  auto loopExe = Executor::create(loop, qctx_.get());
  for (size_t i = 0; i < 5; ++i) {
    auto f = loopExe->execute();
    auto status = std::move(f).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(loop->outputVar());
    auto& value = result.value();
    EXPECT_TRUE(value.isBool());
    EXPECT_TRUE(value.getBool());
  }

  auto f = loopExe->execute();
  auto status = std::move(f).get();
  EXPECT_TRUE(status.ok());
  auto& result = qctx_->ectx()->getResult(loop->outputVar());
  auto& value = result.value();
  EXPECT_TRUE(value.isBool());
  EXPECT_FALSE(value.getBool());
}

}  // namespace graph
}  // namespace nebula

int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);LAGS_max_rank);
  if (FLAGS_go_record) {
    nebula::fs::TempDir rootPath("/tmp/FindShortestPathBenchmark.XXXXXX");
    nebula::storage::setUp(rootPath.path(), FLAGS_max_rank);

    folly::runBenchmarks();
  }
  gCluster.reset();
  return 0;
}
