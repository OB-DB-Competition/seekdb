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
#include "ob_expr_ai_embed.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{
ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, 
                        T_FUN_SYS_AI_EMBED, 
                        N_AI_EMBED, 
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL, 
                        NOT_ROW_DIMENSION) 
{
}

ObExprAIEmbed::~ObExprAIEmbed() 
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (!ob_is_text_tc(types_stack[CONTENT_IDX].get_type())) {
      types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
    }
    types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (param_num == 3) {
      if (ob_is_integer_type(types_stack[DIM_IDX].get_type())) {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be an integer, not a decimal or float");
      }
    }
    type.set_type(ObLongTextType);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_dim = nullptr;
  if (expr.arg_cnt_ == 3 ? OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_dim))
                         : OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content))) {
  } else if (arg_model_id->is_null() || arg_content->is_null()) {
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo resolved_endpoint;
    const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
    query::ObIAiEndpointResolver *endpoint_resolver =
        ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
    ObString model_id = arg_model_id->get_string();
    ObString content;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, temp_allocator,
        *arg_content, expr.args_[CONTENT_IDX]->datum_meta_,
        expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), content))) { return ret; }
    if (model_id.empty() || content.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
      res.set_null();
    }
    int64_t dim = 0;
    ObJsonInt *dim_json = nullptr;
    ObJsonObject *config = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(arg_dim)) {
      dim = arg_dim->is_null() ? 0 : arg_dim->get_int();
      if (dim <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
        res.set_null();
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, dim, dim_json))) {
      } else if (OB_FAIL(config->add("dimensions", dim_json))) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
    } else if (OB_ISNULL(endpoint_resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                   model_id, temp_allocator, resolved_endpoint))) {
    } else {
      ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
      ObString result;
      if (OB_FAIL(model.call_dense_embedding(content, config, result))) {
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                           const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObBitVector &flags = expr.get_evaluated_flags(ctx);
  for (int64_t a = 0; OB_SUCC(ret) && a < expr.arg_cnt_; ++a) {
    if (OB_FAIL(expr.args_[a]->eval_batch(ctx, skip, batch_size))) {}
  }
  ObEvalCtx::TempAllocGuard guard(ctx);
  MultimodeAlloctor allocator(guard.get_allocator());
  ObArray<ObString> models, texts, outputs;
  ObArray<int64_t> dimensions, pending;
  // Stage every input before any network request. The row number is retained
  // across grouping, splitting and provider response reordering.
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    ObString model, text;
    int64_t dim = 0;
    bool active = !skip.at(i) && !flags.at(i);
    if (active) {
      ObDatum &m = expr.args_[MODEL_IDX]->locate_expr_datum(ctx, i);
      ObDatum &t = expr.args_[CONTENT_IDX]->locate_expr_datum(ctx, i);
      if (m.is_null() || t.is_null()) { active = false; }
      else {
        model = m.get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator, t,
            expr.args_[CONTENT_IDX]->datum_meta_, expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), text))) {}
        if (OB_SUCC(ret) && expr.arg_cnt_ == 3) {
          ObDatum &d = expr.args_[DIM_IDX]->locate_expr_datum(ctx, i);
          dim = d.is_null() ? 0 : d.get_int();
          if (dim <= 0) { ret = OB_INVALID_ARGUMENT; }
        }
        if (OB_SUCC(ret) && (model.empty() || text.empty())) { ret = OB_INVALID_ARGUMENT; }
      }
    }
    if (OB_FAIL(ret)) {}
    else if (OB_FAIL(models.push_back(model))) {}
    else if (OB_FAIL(texts.push_back(text))) {}
    else if (OB_FAIL(outputs.push_back(ObString()))) {}
    else if (OB_FAIL(dimensions.push_back(dim))) {}
    else if (OB_FAIL(pending.push_back(active ? 1 : 0))) {}
  }
  auto *resolver = ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    if (!pending.at(i)) { continue; }
    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo endpoint;
    if (!resolver) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, models.at(i), info))) {}
    else if (OB_FAIL(resolver->resolve_by_model_name(models.at(i), allocator, endpoint))) {}
    else {
      ObAIFuncModel model(allocator, *info, endpoint);
      ObJsonObject *config = nullptr;
      ObJsonInt *dim = nullptr;
      if (dimensions.at(i) > 0) {
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {}
        else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, dimensions.at(i), dim))) {}
        else if (OB_FAIL(config->add("dimensions", dim))) {}
      }
      ObArray<ObString> inputs, results;
      ObArray<int64_t> rows;
      for (int64_t j = i; OB_SUCC(ret) && j < batch_size; ++j) {
        if (!pending.at(j) || models.at(j) != models.at(i) || dimensions.at(j) != dimensions.at(i)) { continue; }
        if (OB_FAIL(inputs.push_back(texts.at(j)))) {}
        else if (OB_FAIL(rows.push_back(j))) {}
        else { pending.at(j) = 0; }
        if (OB_SUCC(ret) && (inputs.count() == 20 || j == batch_size - 1)) {
          if (OB_FAIL(model.call_dense_embedding_vector_v2(inputs, config, results))) {}
          else {
            for (int64_t k = 0; k < rows.count(); ++k) { outputs.at(rows.at(k)) = results.at(k); }
          }
          inputs.reuse(); results.reuse(); rows.reuse();
        }
      }
      if (OB_SUCC(ret) && !inputs.empty()) {
        if (OB_FAIL(model.call_dense_embedding_vector_v2(inputs, config, results))) {}
        else {
          for (int64_t k = 0; k < rows.count(); ++k) { outputs.at(rows.at(k)) = results.at(k); }
        }
      }
    }
  }
  // A failed request never marks any row evaluated.
  ObEvalCtx::BatchInfoScopeGuard batch_guard(ctx);
  batch_guard.set_batch_size(batch_size);
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    if (skip.at(i) || flags.at(i)) { continue; }
    batch_guard.set_batch_idx(i);
    ObDatum &result = expr.locate_expr_datum(ctx, i);
    if (outputs.at(i).empty()) { result.set_null(); }
    else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, result, outputs.at(i)))) {}
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < batch_size; ++i) { if (!skip.at(i)) { flags.set(i); } }
  }
  if (ret == OB_INVALID_ARGUMENT) { LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, invalid input or dimension"); }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const 
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOT_NULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   ObObj const_data;
  //   bool got_data = false;
  //   ObAIFuncExprInfo *info = nullptr;
  //   if (OB_ISNULL(allocator)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("allocator is null", K(ret));
  //   } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
  //                                                         model_key,
  //                                                         const_data,
  //                                                         got_data,
  //                                                         *allocator))) {
  //     LOG_WARN("failed to calc offset expr", K(ret));
  //   } else if (!got_data || const_data.is_null()) {
  //   } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //     LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //   } else {
  //     rt_expr.extra_info_ = info;
  //   }
  // }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
    rt_expr.eval_batch_func_ = ObExprAIEmbed::eval_ai_embed_batch;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
