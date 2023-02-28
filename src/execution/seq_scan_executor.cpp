//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      heap_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_.get()),
      iterator_(heap_->Begin(exec_ctx->GetTransaction())) {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == heap_->End()) {
    return false;
  }
  *tuple = Tuple(*iterator_);
  *rid = iterator_->GetRid();
  iterator_++;
  return true;
}

}  // namespace bustub
