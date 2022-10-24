/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>
#include <gtest/gtest.h>

#include <memory>

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
class FindShortestPath : public testing::Test {
 protected:
  Path createPath(const std::vector<std::string>& steps) {
    Path path;
    path.src = Vertex(steps[0], {});
    for (size_t i = 1; i < steps.size(); ++i) {
      path.steps.emplace_back(Step(Vertex(steps[i], {}), EDGE_TYPE, "like", EDGE_RANK, {}));
    }
    return path;
  }

    // Topology is below
  // a->b, a->c
  // b->a, b->c
  // c->a, c->f, c->g
  // d->a, d->c, d->e
  // e->b
  // f->h
  // g->f, g->h, g->k
  // h->x, k->x


  void singleSourceInit() {
    // From: {a}  To: {x}
    {  //  1 step
       //  From: a->b, a->c
      DataSet ds;
      ds.colNames = gnColNames_;
      Row row;
      row.values.emplace_back("a");
      row.values.emplace_back(Value());
      List edges;
      for (const auto& dst : {"b", "c"}) {
        List edge;
        edge.values.emplace_back(EDGE_TYPE);
        edge.values.emplace_back(dst);
        edge.values.emplace_back(EDGE_RANK);
        edges.values.emplace_back(std::move(edge));
      }
      row.values.emplace_back(edges);
      row.values.emplace_back(Value());
      ds.rows.emplace_back(std::move(row));
      single1StepFrom_ = std::move(ds);

      // To: x<-h, x<-k
      DataSet ds1;
      ds1.colNames = gnColNames_;
      Row row1;
      row1.values.emplace_back("x");
      row1.values.emplace_back(Value());
      List edges1;
      for (const auto& dst : {"h", "k"}) {
        List edge;
        edge.values.emplace_back(-EDGE_TYPE);
        edge.values.emplace_back(dst);
        edge.values.emplace_back(EDGE_RANK);
        edges1.values.emplace_back(std::move(edge));
      }
      row1.values.emplace_back(edges1);
      row1.values.emplace_back(Value());
      ds1.rows.emplace_back(std::move(row1));
      single1StepTo_ = std::move(ds1);
    }
    {  // 2 step
       // From: b->a, b->c, c->a, c->f, c->g
      DataSet ds;
      ds.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data(
          {{"b", {"a", "c"}}, {"c", {"a", "f", "g"}}});
      for (const auto& src : data) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds.rows.emplace_back(std::move(row));
      }
      single2StepFrom_ = std::move(ds);

      // To : h<-f, h<-g, k<-g
      DataSet ds1;
      ds1.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data1(
          {{"h", {"f", "g"}}, {"k", {"g"}}});
      for (const auto& src : data1) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(-EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
      }
      single2StepTo_ = std::move(ds1);
    }
  }


    void SetUp() override {
    qctx_ = std::make_unique<QueryContext>();
    singleSourceInit();
  }

  protected:
  std::unique_ptr<QueryContext> qctx_;
  const int EDGE_TYPE = 1;
  const int EDGE_RANK = 0;
  DataSet single1StepFrom_;
  DataSet single1StepTo_;
  DataSet single2StepFrom_;
  DataSet single2StepTo_;
  const std::vector<std::string> pathColNames_ = {"path"};
  const std::vector<std::string> gnColNames_ = {
      kVid, "_stats", "_edge:+like:_type:_dst:_rank", "_expr"};
};

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
