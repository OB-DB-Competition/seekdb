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
#include "ob_expr_ai_rerank.h"
#include <algorithm>
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "lib/utility/utility.h"
#include "common/json_type/ob_json_common.h"
 
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{

ObExprAIRerank::ObExprAIRerank(common::ObIAllocator &alloc)
: ObFuncExprOperator(alloc, 
                    T_FUN_SYS_AI_RERANK, 
                    N_AI_RERANK, 
                    MORE_THAN_ZERO,
                    NOT_VALID_FOR_GENERATED_COL, 
                    NOT_ROW_DIMENSION) 
{
}

ObExprAIRerank::~ObExprAIRerank() 
{
}

int ObExprAIRerank::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_stack,
                                    int64_t param_num,
                                    common::ObExprTypeCtx &type_ctx) const {
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 3 || param_num > 4)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    if (!ob_is_string_tc(types_stack[MODEL_IDX].get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model key must be string type");
    } else if (!ob_is_string_tc(types_stack[QUERY_IDX].get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, query must be string type");
    } else {
      types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      types_stack[QUERY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    ObObjType in_type = types_stack[DOCUMENTS_IDX].get_type();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, DOCUMENTS_IDX, N_AI_RERANK))) {
    } else if (ob_is_string_type(in_type) && types_stack[DOCUMENTS_IDX].get_collation_type() != CS_TYPE_BINARY) {
      if (types_stack[DOCUMENTS_IDX].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[DOCUMENTS_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (param_num == 4) {
      ObObjType doc_key_type = types_stack[DOC_KEY_IDX].get_type();
      if (doc_key_type == ObNullType) {
      } else if (doc_key_type == ObJsonType) {
      } else if (ob_is_string_type(doc_key_type)) {
        types_stack[DOC_KEY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else {  
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, doc key must be string type");
      }
    }

    if (OB_SUCC(ret)) {
      type.set_json();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    }
  }
  return ret;
}

int ObExprAIRerank::eval_ai_rerank(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
int ret = OB_SUCCESS;
  ObDatum *m = nullptr, *q = nullptr, *d = nullptr, *k = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, m, q, d, k))) { return ret; }
  if (m->is_null() || q->is_null() || d->is_null()) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, required parameter is null");
    return OB_INVALID_ARGUMENT;
  }
  ObEvalCtx::TempAllocGuard guard(ctx);
  MultimodeAlloctor allocator(guard.get_allocator());
  ObString model_id = m->get_string(), query = q->get_string(), key;
  const bool objects = k && !k->is_null();
  ObIJsonBase *base = nullptr;
  bool is_null = false;
  ObJsonArray *documents = nullptr, *texts = nullptr, *out = nullptr;
  if (objects) {
    if (ob_is_json(expr.args_[DOC_KEY_IDX]->datum_meta_.type_)) { return OB_INVALID_ARGUMENT; }
    key = k->get_string();
    if (key.empty()) { return OB_INVALID_ARGUMENT; }
  }
  if (model_id.empty() || query.empty()) { ret = OB_INVALID_ARGUMENT; }
  else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, DOCUMENTS_IDX, base, is_null))) {}
  else if (!base || !base->is_tree() || base->json_type() != ObJsonNodeType::J_ARRAY || base->element_count() == 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    documents = static_cast<ObJsonArray *>(base);
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, texts))) {}
    for (int64_t i = 0; OB_SUCC(ret) && i < documents->element_count(); ++i) {
      ObJsonNode *text = documents->get_value(i);
      if (objects) {
        text = text && text->json_type() == ObJsonNodeType::J_OBJECT ? static_cast<ObJsonObject *>(text)->get_value(key) : nullptr;
      }
      if (!text || text->json_type() != ObJsonNodeType::J_STRING) { ret = OB_INVALID_ARGUMENT; }
      else if (OB_FAIL(texts->append(text))) {}
    }
  }
  ObAIFuncExprInfo *info = nullptr;
  share::ObAiModelEndpointInfo endpoint;
  auto *resolver = ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
  if (OB_FAIL(ret)) {}
  else if (!resolver) { ret = OB_ERR_UNEXPECTED; }
  else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_id, info))) {}
  else if (OB_FAIL(resolver->resolve_by_model_name(model_id, allocator, endpoint))) {}
  else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, out))) {}
  struct RankedDocument { int64_t index; double score; ObJsonNode *score_node; ObJsonObject *response; };
  ObArray<RankedDocument> ranked;
  if (OB_SUCC(ret)) {
    ObAIFuncModel model(allocator, *info, endpoint);
    for (int64_t start = 0; OB_SUCC(ret) && start < texts->element_count(); start += 20) {
      ObJsonArray *batch = nullptr, *response = nullptr;
      if (OB_FAIL(construct_batch_document_array(allocator, texts, start, std::min<int64_t>(start + 20, texts->element_count()), batch))) {}
      else if (OB_FAIL(model.call_rerank(query, batch, response))) {}
      else {
        // call_rerank validates count, index uniqueness/range and finite scores.
        for (int64_t i = 0; OB_SUCC(ret) && i < response->element_count(); ++i) {
          ObJsonObject *item = static_cast<ObJsonObject *>(response->get_value(i));
          ObJsonNode *score = item->get_value(SCORE_KEY);
          double value = score->json_type() == ObJsonNodeType::J_INT ? static_cast<ObJsonInt *>(score)->get_int()
              : score->json_type() == ObJsonNodeType::J_UINT ? static_cast<ObJsonUint *>(score)->get_uint()
              : static_cast<ObJsonDouble *>(score)->get_double();
          RankedDocument row{start + static_cast<ObJsonInt *>(item->get_value(INDEX_KEY))->get_int(), value, score, item};
          if (OB_FAIL(ranked.push_back(row))) {}
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(ranked.begin(), ranked.end(), [](const RankedDocument &a, const RankedDocument &b) {
      return a.score == b.score ? a.index < b.index : a.score > b.score;
    });
    for (int64_t i = 0; OB_SUCC(ret) && i < ranked.count(); ++i) {
      RankedDocument &row = ranked.at(i);
      ObJsonObject *item = nullptr;
      if (objects) {
        item = static_cast<ObJsonObject *>(documents->get_value(row.index)->clone(&allocator, true));
        if (!item) { ret = OB_ALLOCATE_MEMORY_FAILED; }
        else if (OB_FAIL(item->add(MODEL_SCORE_KEY, row.score_node))) {}
      } else {
        item = row.response;
        ObJsonInt *index = nullptr;
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, row.index, index))) {}
        else if (OB_FAIL(item->add(INDEX_KEY, index))) {}
      }
      if (OB_SUCC(ret) && OB_FAIL(out->append(item))) {}
    }
    ObString raw;
    if (OB_FAIL(ret)) {}
    else if (OB_FAIL(out->get_raw_binary(raw, &allocator))) {}
    else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw))) {}
  }
  if (ret == OB_INVALID_ARGUMENT) { LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, invalid documents or text field"); }
  return ret;
}

int ObExprAIRerank::construct_batch_document_array(ObIAllocator &allocator, ObJsonArray *document_array, int64_t start_idx, int64_t end_idx, ObJsonArray *&batch_document_array)
{
  INIT_SUCC(ret);
  ObJsonArray *result_array = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
  } else {
    for (int64_t i = start_idx; i < end_idx; i++) {
      if (OB_FAIL(result_array->append(document_array->get_value(i)))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    batch_document_array = result_array;
  }
  return ret;
}

int ObExprAIRerank::cg_expr(ObExprCGCtx &expr_cg_ctx, 
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
  //   bool got_data = false;
  //   ObObj const_data;
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
  //   } else {
  //     ObAIFuncExprInfo *info = OB_NEWx(ObAIFuncExprInfo, allocator, *allocator, T_FUN_SYS_AI_RERANK);
  //     if (OB_ISNULL(info)) {
  //       ret = OB_ALLOCATE_MEMORY_FAILED;
  //       LOG_WARN("failed to allocate memory for ai rerank info", K(ret));
  //     } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //       LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //     } else {
  //       rt_expr.extra_info_ = info;
  //     }
  //   }
  // }
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIRerank::eval_ai_rerank;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
