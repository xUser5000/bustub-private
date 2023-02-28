#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Schema s = child_executor_->GetOutputSchema();
  Tuple t;
  RID r;
  child_executor_->Init();
  while (child_executor_->Next(&t, &r)) {
    result_set_.push_back(t);
  }
  std::sort(result_set_.begin(), result_set_.end(), [this, &s](const Tuple &a, const Tuple &b) {
    /*
     * loop over all group by expressions i
     *  if a[i] == b[i] in terms of the current expression -> continue
     *  if order == "default" -> order = "ASC"
     *  if a[i] < b[i] && order == "ASC" -> return true
     *  if a[i] > b[i] && order == "Desc" -> return true
     * return false
     * */
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
    // Equal in terms of all order by expressions
    return true;
  });
  iterator_ = result_set_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == result_set_.end()) {
    return false;
  }
  *tuple = *iterator_;
  *rid = tuple->GetRid();
  iterator_++;
  return true;
}

}  // namespace bustub
