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
  : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    auto [meta, tp] = iter_->GetTuple();
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&tp, table_info_->schema_);
      if (!value.IsNull() && !value.GetAs<bool>()) {
        ++(*iter_);
        continue;
      }
    }
    if (meta.is_deleted_) {
      ++(*iter_);
      continue;
    }
    *tuple = tp;
    rid->Set(iter_->GetRID().GetPageId(), iter_->GetRID().GetSlotNum());
    ++ (*iter_);
    return true;
  }
  return false;
}

}  // namespace bustub
