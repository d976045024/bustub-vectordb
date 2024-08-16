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
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  Tuple tuple;
  RID rid;
  TupleMeta tuple_meta{};
  auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  
  while (child_executor_->Next(&tuple, &rid)) {
    LOG_DEBUG("tuple is %s", tuple.ToString(&table_info_->schema_).c_str());
    auto insert_rid = table_info_->table_->InsertTuple(tuple_meta, tuple);
    insert_num_++;

    for (auto index_info : index_infos) {
      VectorIndex *vector_index = dynamic_cast<VectorIndex*>(index_info->index_.get());
      Tuple index_tuple = tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      Value value = tuple.GetValue(&table_info_->schema_, index_info->index_->GetKeyAttrs()[0]);
      std::vector<double> vector_value = value.GetVector();
      
      vector_index->InsertVectorEntry(vector_value, insert_rid.value());
    }
  }
  emitted_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (emitted_) {
    return false;
  }
  emitted_ = true;
  *tuple = Tuple{{ValueFactory::GetIntegerValue(insert_num_)}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
