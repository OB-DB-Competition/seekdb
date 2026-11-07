// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"

namespace oceanbase::sql::file_sql {

enum class Format : uint8_t { CSV, JSONL, PARQUET, GLOB };
enum class Type : uint8_t { NIL, INT, DOUBLE, BOOL, TEXT, DATE, DATETIME };
constexpr int64_t MICROS_PER_DAY = 86400000000LL;
constexpr size_t MAX_COLUMNS = 512;
constexpr size_t MAX_FIELD = 1024 * 1024;
constexpr size_t MAX_RECORD = 8 * MAX_FIELD;

struct Options
{
  bool header = true;
  char delimiter = ',';
  char quote = '"';
  char escape = '"';
  std::string null_text;
};

struct Stamp
{
  uint64_t device = 0, inode = 0, size = 0, modified = 0, changed = 0;
  bool operator==(const Stamp &rhs) const
  {
    return device == rhs.device && inode == rhs.inode && size == rhs.size
        && modified == rhs.modified && changed == rhs.changed;
  }
};

struct Column
{
  std::string name;
  Type type = Type::NIL;
  bool nullable = false;
  int16_t scale = 0;
};

struct Plan
{
  Format format = Format::CSV;
  Options options;
  std::string path;
  Stamp stamp;
  uint64_t row_count = 0;
  std::vector<Column> columns;
  std::vector<std::string> matches;

  int encode(common::ObIAllocator &allocator, common::ObString &result) const;
  int decode(common::ObString input);
};

struct Cell
{
  Type type = Type::NIL;
  std::string text;
};

const char *type_name(Type type);
Type merge(Type lhs, Type rhs);
Type classify(std::string_view text, bool infer_number);
bool integer(std::string_view text, int64_t &value);
bool real(std::string_view text, double &value);
bool calendar(std::string_view text, int64_t &micros, int16_t &scale);
bool utf8(std::string_view text);

class Input
{
public:
  Input() = default;
  ~Input();
  Input(const Input &) = delete;
  Input &operator=(const Input &) = delete;
  int open(const std::string &path, const Stamp *expected);
  int check() const;
  int read_at(uint64_t offset, void *data, size_t count) const;
  int byte(int &value);
  void rewind(uint64_t offset = 0) { offset_ = offset; buffer_size_ = 0; }
  void seek(uint64_t offset) { offset_ = offset; }
  uint64_t offset() const { return offset_; }
  const Stamp &stamp() const { return stamp_; }
private:
  int fd_ = -1;
  Stamp stamp_;
  uint64_t offset_ = 0;
  uint64_t buffer_start_ = 0;
  size_t buffer_size_ = 0;
  char buffer_[64 * 1024];
};

int canonical_path(common::ObString secure, const std::string &path, std::string &canonical);
int directory_fd(const std::string &canonical, int &fd);
int infer(common::ObString secure, Plan &plan);

class Parquet;
class Scanner
{
public:
  Scanner() = default;
  ~Scanner();
  Scanner(const Scanner &) = delete;
  Scanner &operator=(const Scanner &) = delete;
  int open(common::ObString secure, common::ObString encoded, const std::vector<int> &projection);
  int rescan();
  int next(std::vector<Cell> &cells);
private:
  Plan plan_;
  Input input_;
  std::vector<int> projection_;
  std::vector<Cell> record_;
  std::vector<std::string> keys_;
  Parquet *parquet_ = nullptr;
  uint64_t row_ = 0;
};

int copy_target(common::ObString secure, std::string &target);

class CsvOutput
{
public:
  CsvOutput() = default;
  ~CsvOutput();
  CsvOutput(const CsvOutput &) = delete;
  CsvOutput &operator=(const CsvOutput &) = delete;
  int open(common::ObString secure, const Plan &plan);
  int write(const std::vector<Cell> &cells);
  int publish();
private:
  int parent_ = -1, file_ = -1;
  std::string basename_, temp_;
  Options options_;
};

}
