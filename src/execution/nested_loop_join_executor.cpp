//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  Tuple t;
  RID r;

  left_executor_->Init();
  std::vector<Tuple> left_set;
  while (left_executor_->Next(&t, &r)) {
    left_set.push_back(t);
  }
  size_t n = left_set.size();

  right_executor_->Init();
  std::vector<Tuple> right_set;
  while (right_executor_->Next(&t, &r)) {
    right_set.push_back(t);
  }
  size_t m = right_set.size();

  for (size_t i = 0; i < n; i++) {
    bool found = false;
    for (size_t j = 0; j < m; j++) {
      auto match = plan_->Predicate().EvaluateJoin(&left_set[i], left_executor_->GetOutputSchema(), &right_set[j],
                                                   right_executor_->GetOutputSchema());
      if (!match.IsNull() && match.GetAs<bool>()) {
        found = true;
        Tuple ans = MergeTuples(left_set[i], left_executor_->GetOutputSchema(), right_set[j],
                                right_executor_->GetOutputSchema());
        result_set_.push_back(ans);
      }
    }
    if (!found && plan_->GetJoinType() == JoinType::LEFT) {
      Tuple null_tuple = GetNullTuple(right_executor_->GetOutputSchema());
      Tuple ans =
          MergeTuples(left_set[i], left_executor_->GetOutputSchema(), null_tuple, right_executor_->GetOutputSchema());
      result_set_.push_back(ans);
    }
  }
  iterator_ = result_set_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == result_set_.end()) {
    return false;
  }
  *tuple = *iterator_;
  *rid = tuple->GetRid();
  iterator_++;
  return true;
}

auto NestedLoopJoinExecutor::MergeTuples(const Tuple &left_tuple, const Schema &left_schema, const Tuple &right_tuple,
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
auto NestedLoopJoinExecutor::GetNullTuple(const Schema &s) -> Tuple {
  std::vector<Value> values;
  for (size_t i = 0; i < s.GetColumnCount(); i++) {
    values.push_back(ValueFactory::GetNullValueByType(s.GetColumn(i).GetType()));
  }
  return {values, &s};
}

}  // namespace bustub
