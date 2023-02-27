//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  Tuple t;
  RID r;
  child_executor_->Init();
  std::vector<Tuple> left_set;
  while (child_executor_->Next(&t, &r)) {
    left_set.push_back(t);
  }
  Schema left_schema = child_executor_->GetOutputSchema();

  Index *index = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get();
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  TableHeap *heap = table_info->table_.get();
  Schema right_schema = table_info->schema_;

  for (auto &left_tuple : left_set) {
    Value v = plan_->KeyPredicate()->Evaluate(&left_tuple, left_schema);
    Tuple key = Tuple({v}, index->GetKeySchema());
    std::vector<RID> res;
    index->ScanKey(key, &res, exec_ctx_->GetTransaction());
    bool found = false;
    if (!res.empty()) {
      RID rid = res.back();
      Tuple right_tuple;
      heap->GetTuple(rid, &right_tuple, exec_ctx_->GetTransaction());
      Value match = plan_->KeyPredicate()->EvaluateJoin(&left_tuple, child_executor_->GetOutputSchema(), &right_tuple,
                                                        right_schema);
      found = true;
      Tuple merged_tuple = MergeTuples(left_tuple, child_executor_->GetOutputSchema(), right_tuple, right_schema);
      result_set_.push_back(merged_tuple);
    }

    if (!found && plan_->GetJoinType() == JoinType::LEFT) {
      Tuple right_tuple = GetNullTuple(right_schema);
      Tuple merged_tuple = MergeTuples(left_tuple, left_schema, right_tuple, right_schema);
      result_set_.push_back(merged_tuple);
    }
  }
  iterator_ = result_set_.begin();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == result_set_.end()) {
    return false;
  }
  *tuple = *iterator_;
  *rid = tuple->GetRid();
  iterator_++;
  return true;
}

auto NestIndexJoinExecutor::MergeTuples(const Tuple &left_tuple, const Schema &left_schema, const Tuple &right_tuple,
                                        const Schema &right_schema) -> Tuple {
  std::vector<Column> a = left_schema.GetColumns();
  std::vector<Column> b = right_schema.GetColumns();
  std::vector<Column> columns;
  columns.insert(columns.end(), a.begin(), a.end());
  columns.insert(columns.end(), b.begin(), b.end());
  Schema s(columns);
  std::vector<Value> values;
  for (size_t k = 0; k < left_schema.GetColumnCount(); k++) {
    values.push_back(left_tuple.GetValue(&left_schema, k));
  }
  for (size_t k = 0; k < right_schema.GetColumnCount(); k++) {
    values.push_back(right_tuple.GetValue(&right_schema, k));
  }

  return {values, &s};
}
auto NestIndexJoinExecutor::GetNullTuple(const Schema &s) -> Tuple {
  std::vector<Value> values;
  for (size_t i = 0; i < s.GetColumnCount(); i++) {
    values.push_back(ValueFactory::GetNullValueByType(s.GetColumn(i).GetType()));
  }
  return {values, &s};
}

}  // namespace bustub
