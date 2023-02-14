//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  used_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (used_) {
    return false;
  }

  TableInfo *info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  TableHeap *heap = info->table_.get();
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(info->name_);

  Tuple t;
  RID r;
  int tuples_count = 0;
  while (child_executor_->Next(&t, &r)) {
    heap->InsertTuple(t, &r, exec_ctx_->GetTransaction());
    for (auto &i : indexes) {
      i->index_->InsertEntry(t, r, exec_ctx_->GetTransaction());
    }
    tuples_count++;
  }

  Schema s({Column("insert_rows", TypeId::INTEGER)});
  *tuple = Tuple({Value(TypeId::INTEGER, tuples_count)}, &s);
  used_ = true;
  return true;
}

}  // namespace bustub
