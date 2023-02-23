//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan->aggregates_, plan->agg_types_)),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  bool empty_child = true;
  Tuple t;
  RID r;
  child_->Init();
  while (child_->Next(&t, &r)) {
    AggregateKey aggregate_key = MakeAggregateKey(&t);
    AggregateValue aggregate_value = MakeAggregateValue(&t);
    aht_.InsertCombine(aggregate_key, aggregate_value);
    empty_child = false;
  }
  if (empty_child && plan_->group_bys_.empty()) {
    aht_.InsertEmptyEntry();
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  Schema s = GetOutputSchema();
  std::vector<Value> group_bys = aht_iterator_.Key().group_bys_;
  std::vector<Value> aggregates = aht_iterator_.Val().aggregates_;
  std::vector<Value> values;
  values.insert(values.end(), group_bys.begin(), group_bys.end());
  values.insert(values.end(), aggregates.begin(), aggregates.end());
  *tuple = Tuple(values, &s);
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
