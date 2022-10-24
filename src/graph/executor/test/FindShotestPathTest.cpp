/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

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



TEST_F(FindShortestPath, singleSourceShortestPath) {
  int steps = 5;
  std::string leftVidVar = "leftVid";
  std::string rightVidVar = "rightVid";
  std::string fromGNInput = "fromGNInput";
  std::string toGNInput = "toGNInput";
  qctx_->symTable()->newVariable(fromGNInput);
  qctx_->symTable()->newVariable(toGNInput);
  {
    qctx_->symTable()->newVariable(leftVidVar);
    DataSet fromVid;
    fromVid.colNames = {nebula::kVid};
    Row row;
    row.values.emplace_back("a");
    fromVid.rows.emplace_back(std::move(row));
    ResultBuilder builder;
    builder.value(std::move(fromVid)).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(leftVidVar, builder.build());
  }
  {
    qctx_->symTable()->newVariable(rightVidVar);
    DataSet toVid;
    toVid.colNames = {nebula::kVid};
    Row row;
    row.values.emplace_back("x");
    toVid.rows.emplace_back(std::move(row));
    ResultBuilder builder;
    builder.value(std::move(toVid)).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(rightVidVar, builder.build());
  }
  auto fromGN = StartNode::make(qctx_.get());
  auto toGN = StartNode::make(qctx_.get());

  auto* path = BFSShortestPath::make(qctx_.get(), fromGN, toGN, steps);
  path->setLeftVar(fromGNInput);
  path->setRightVar(toGNInput);
  path->setLeftVidVar(leftVidVar);
  path->setRightVidVar(rightVidVar);
  path->setColNames(pathColNames_);

  auto pathExe = std::make_unique<BFSShortestPathExecutor>(path, qctx_.get());
  // Step 1
  {
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(single1StepFrom_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(fromGNInput, builder.build());
    }
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(single1StepTo_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(toGNInput, builder.build());
    }
    auto future = pathExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(path->outputVar());

    DataSet expected;
    expected.colNames = pathColNames_;
    auto resultDs = result.value().getDataSet();
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
    {
      DataSet expectLeftVid;
      expectLeftVid.colNames = {nebula::kVid};
      for (const auto& vid : {"b", "c"}) {
        Row row;
        row.values.emplace_back(vid);
        expectLeftVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(leftVidVar);
      auto resultLeftVid = resultVid.value().getDataSet();
      std::sort(resultLeftVid.rows.begin(), resultLeftVid.rows.end());
      std::sort(expectLeftVid.rows.begin(), expectLeftVid.rows.end());
      EXPECT_EQ(resultLeftVid, expectLeftVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    {
      DataSet expectRightVid;
      expectRightVid.colNames = {nebula::kVid};
      for (const auto& vid : {"h", "k"}) {
        Row row;
        row.values.emplace_back(vid);
        expectRightVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(rightVidVar);
      auto resultRightVid = resultVid.value().getDataSet();
      std::sort(resultRightVid.rows.begin(), resultRightVid.rows.end());
      std::sort(expectRightVid.rows.begin(), expectRightVid.rows.end());
      EXPECT_EQ(resultRightVid, expectRightVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
  }
  // 2 Step
  {
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(single2StepFrom_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(fromGNInput, builder.build());
    }
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(single2StepTo_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(toGNInput, builder.build());
    }
    auto future = pathExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(path->outputVar());

    DataSet expected;
    expected.colNames = pathColNames_;
    std::vector<std::vector<std::string>> paths(
        {{"a", "c", "f", "h", "x"}, {"a", "c", "g", "h", "x"}, {"a", "c", "g", "k", "x"}});
    for (const auto& p : paths) {
      Row row;
      row.values.emplace_back(createPath(p));
      expected.rows.emplace_back(std::move(row));
    }
    auto resultDs = result.value().getDataSet();
    std::sort(expected.rows.begin(), expected.rows.end());
    std::sort(resultDs.rows.begin(), resultDs.rows.end());
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
    {
      DataSet expectLeftVid;
      expectLeftVid.colNames = {nebula::kVid};
      for (const auto& vid : {"f", "g"}) {
        Row row;
        row.values.emplace_back(vid);
        expectLeftVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(leftVidVar);
      auto resultLeftVid = resultVid.value().getDataSet();
      std::sort(resultLeftVid.rows.begin(), resultLeftVid.rows.end());
      std::sort(expectLeftVid.rows.begin(), expectLeftVid.rows.end());
      EXPECT_EQ(resultLeftVid, expectLeftVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    {
      DataSet expectRightVid;
      expectRightVid.colNames = {nebula::kVid};
      for (const auto& vid : {"f", "g"}) {
        Row row;
        row.values.emplace_back(vid);
        expectRightVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(rightVidVar);
      auto resultRightVid = resultVid.value().getDataSet();
      std::sort(resultRightVid.rows.begin(), resultRightVid.rows.end());
      std::sort(expectRightVid.rows.begin(), expectRightVid.rows.end());
      EXPECT_EQ(resultRightVid, expectRightVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
  }
}


}// namespace graph;





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
