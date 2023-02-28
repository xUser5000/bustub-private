#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  Schema s = child_executor_->GetOutputSchema();
  auto compare_function = [this, &s](const Tuple &a, const Tuple &b) {
    for (auto &p : plan_->GetOrderBy()) {
      OrderByType order = p.first;
      AbstractExpressionRef expression = p.second;
      Value x = expression->Evaluate(&a, s);
      Value y = expression->Evaluate(&b, s);
      if (x.CompareEquals(y) == CmpBool::CmpTrue) {
        continue;
      }
      if (order == OrderByType::DEFAULT) {
        order = OrderByType::ASC;
      }
      if (x.CompareLessThan(y) == CmpBool::CmpTrue && order == OrderByType::ASC) {
        return true;
      }
      if (x.CompareGreaterThan(y) == CmpBool::CmpTrue && order == OrderByType::DESC) {
        return true;
      }
      return false;
    }
    return true;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(compare_function)> q(compare_function);
  Tuple t;
  RID r;
  child_executor_->Init();
  while (child_executor_->Next(&t, &r)) {
    q.push(t);
    if (q.size() > plan_->GetN()) {
      q.pop();
    }
  }
  while (!q.empty()) {
    result_set_.push_back(q.top());
    q.pop();
  }
  std::reverse(result_set_.begin(), result_set_.end());
  iterator_ = result_set_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == result_set_.end()) {
    return false;
  }
  *tuple = *iterator_;
  *rid = tuple->GetRid();
  iterator_++;
  return true;
}

}  // namespace bustub
