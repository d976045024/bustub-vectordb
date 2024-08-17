#include "execution/executors/sort_executor.h"
#include "execution/expressions/column_value_expression.h"
namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
    rids_.push_back(rid);
  }
  auto order_bys = plan_->GetOrderBy();
  const Schema &schema = plan_->OutputSchema();
  std::sort(tuples_.begin(), tuples_.end(), [&](const Tuple &t1, const Tuple &t2) {
    for (auto order_by : order_bys) {
      OrderByType type = order_by.first;
      AbstractExpression *expr;
      expr = dynamic_cast<VectorExpression*>(order_by.second.get());
      if (expr == nullptr) {
        expr = dynamic_cast<ColumnValueExpression*>(order_by.second.get());
      }
      if (expr != nullptr) {
        auto dist1 = expr->Evaluate(&t1, schema);
        auto dist2 = expr->Evaluate(&t2, schema);
        if (dist1.CompareEquals(dist2) == CmpBool::CmpTrue) {
          continue;
        }
        switch (type) {
        case OrderByType::DESC:
          if (dist1.CompareGreaterThan(dist2) == CmpBool::CmpTrue) {
            return true;
          } else {
            return false;
          }
        default:
          if (dist1.CompareLessThan(dist2) == CmpBool::CmpTrue) {
            return true;
          } else {
            return false;
          }
        }
      }
    }
    return false;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.size()) {
    return false;
  }
  *tuple = tuples_[iter_];
  iter_++;
  return true;
}

}  // namespace bustub
