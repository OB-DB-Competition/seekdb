/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/basic/ob_file_sql.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/pl/ob_pl_user_type.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_, file_plan_, file_columns_);

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (!MY_SPEC.file_plan_.empty()) {
    bool enabled = false;
    ObString secure;
    ObSQLSessionInfo *session = ctx_.get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {
    } else if (!enabled) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL requires enable_file_sql");
    } else if (OB_FAIL(session->get_secure_file_priv(secure))) {
    } else {
      std::vector<int> projection;
      for (int64_t i = 0; i < MY_SPEC.file_columns_.count(); ++i) {
        projection.push_back(MY_SPEC.file_columns_.at(i));
      }
      file_scanner_ = new (std::nothrow) file_sql::Scanner;
      if (OB_ISNULL(file_scanner_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(file_scanner_->open(secure, MY_SPEC.file_plan_, projection))) {
        delete file_scanner_;
        file_scanner_ = nullptr;
      }
    }
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else if (file_scanner_ != nullptr) {
    ret = file_scanner_->rescan();
  } else {
    node_idx_ = 0;
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  delete file_scanner_;
  file_scanner_ = nullptr;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  return ret;
}

void ObFunctionTableOp::destroy()
{
  delete file_scanner_;
  file_scanner_ = nullptr;
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  if (file_scanner_) {
    clear_evaluated_flag();
    return inner_get_next_file_row(0);
  }
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_batch(int64_t max_row_cnt)
{
  if (file_scanner_ == nullptr) { return ObOperator::inner_get_next_batch(max_row_cnt); }
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_size(max_row_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < max_row_cnt; ++i) {
    guard.set_batch_idx(i);
    if (OB_FAIL(inner_get_next_file_row(i))) {
      if (ret == OB_ITER_END) {
        brs_.end_ = true;
        ret = OB_SUCCESS;
      }
      break;
    }
    ++brs_.size_;
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_file_row(int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  std::vector<file_sql::Cell> cells;
  if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(file_scanner_->next(cells))) {
  } else if (cells.size() != MY_SPEC.column_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
  }
  for (size_t i = 0; OB_SUCC(ret) && i < cells.size(); ++i) {
    ObExpr &expr = *MY_SPEC.column_exprs_.at(i);
    ObDatum &datum = expr.locate_datum_for_write(eval_ctx_);
    const auto &cell = cells[i];
    int64_t integer = 0;
    double real = 0;
    int16_t scale = 0;
    if (cell.type == file_sql::Type::NIL) {
      datum.set_null();
    } else {
      switch (expr.datum_meta_.type_) {
        case ObIntType:
          if (!file_sql::integer(cell.text, integer)) { ret = OB_INVALID_ARGUMENT; }
          else { datum.set_int(integer); }
          break;
        case ObTinyIntType:
          datum.set_int(ObString(cell.text.size(), cell.text.data()).case_compare("true") == 0);
          break;
        case ObDoubleType:
          if (!file_sql::real(cell.text, real)) { ret = OB_INVALID_ARGUMENT; }
          else { datum.set_double(real); }
          break;
        case ObDateType:
        case ObDateTimeType:
          if (!file_sql::calendar(cell.text, integer, scale)) {
            ret = OB_INVALID_ARGUMENT;
          } else if (expr.datum_meta_.type_ == ObDateType) {
            datum.set_date(integer / file_sql::MICROS_PER_DAY);
          } else {
            datum.set_datetime(integer);
          }
          break;
        default:
          if (cell.text.empty()) {
            datum.set_string("", 0);
          } else {
            char *mem = expr.get_str_res_mem(eval_ctx_, cell.text.size(), batch_idx);
            if (OB_ISNULL(mem)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(mem, cell.text.data(), cell.text.size());
              datum.set_string(mem, cell.text.size());
            }
          }
          break;
      }
    }
    if (OB_SUCC(ret)) { expr.set_evaluated_projected(eval_ctx_); }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
