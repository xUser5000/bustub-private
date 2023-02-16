//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())),
      iterator_(index_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  heap_ = exec_ctx_->GetCatalog()->GetTable(index_->GetMetadata()->GetTableName())->table_.get();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == index_->GetEndIterator()) {
    return false;
  }
  *rid = (*iterator_).second;
  heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iterator_;
  return true;
}

}  // namespace bustub
