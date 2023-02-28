//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  used_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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
    heap->MarkDelete(r, exec_ctx_->GetTransaction());
    for (auto &i : indexes) {
      IndexMetadata *metadata = i->index_->GetMetadata();
      Tuple key =
          t.KeyFromTuple(child_executor_->GetOutputSchema(), *metadata->GetKeySchema(), metadata->GetKeyAttrs());
      i->index_->DeleteEntry(key, r, exec_ctx_->GetTransaction());
    }
    tuples_count++;
  }

  Schema s({Column("delete_rows", TypeId::INTEGER)});
  *tuple = Tuple({Value(TypeId::INTEGER, tuples_count)}, &s);
  used_ = true;
  return true;
}

}  // namespace bustub
