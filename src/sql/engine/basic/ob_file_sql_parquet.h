// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "sql/engine/basic/ob_file_sql.h"
#include <memory>

namespace oceanbase::sql::file_sql {
class Parquet
{
public:
  explicit Parquet(Input &input);
  ~Parquet();
  int schema(Plan &plan);
  int open(const Plan &plan, const std::vector<int> &projection);
  int rescan();
  int next(std::vector<Cell> &cells);
private:
  struct State;
  std::unique_ptr<State> state_;
};
}
