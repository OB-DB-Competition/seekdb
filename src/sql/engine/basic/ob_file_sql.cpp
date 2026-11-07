// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_sql.h"
#include "sql/engine/basic/ob_file_sql_parquet.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/worker.h"
#include "share/ob_errno.h"
#include <algorithm>
#include <atomic>
#include <charconv>
#include <cmath>
#include <cstring>
#include <memory>
#include <new>
#include <dirent.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <sys/stat.h>
#include <unistd.h>

namespace oceanbase::sql::file_sql {
using namespace common;
namespace {

constexpr size_t MAX_MATCHES = 4096;
constexpr size_t MAX_PLAN = 8 * MAX_FIELD;

ObString view(std::string_view s) { return ObString(s.size(), s.data()); }

int invalid(uint64_t row, size_t col, uint64_t offset, const char *reason)
{
  char message[256];
  snprintf(message, sizeof(message), "file SQL row %llu column %zu byte %llu: %s",
           static_cast<unsigned long long>(row), col,
           static_cast<unsigned long long>(offset), reason);
  LOG_USER_ERROR(OB_INVALID_ARGUMENT, message);
  return OB_INVALID_ARGUMENT;
}

bool digit(char c) { return c >= '0' && c <= '9'; }

int digits(std::string_view text, size_t pos, size_t count)
{
  int value = 0;
  for (size_t i = pos; i < pos + count; ++i) {
    if (!digit(text[i])) { return -1; }
    value = value * 10 + text[i] - '0';
  }
  return value;
}

Stamp file_stamp(const struct stat &st)
{
#ifdef __APPLE__
  const auto &modified = st.st_mtimespec;
  const auto &changed = st.st_ctimespec;
#else
  const auto &modified = st.st_mtim;
  const auto &changed = st.st_ctim;
#endif
  return {static_cast<uint64_t>(st.st_dev), static_cast<uint64_t>(st.st_ino),
          static_cast<uint64_t>(st.st_size),
          static_cast<uint64_t>(modified.tv_sec) * 1000000000 + modified.tv_nsec,
          static_cast<uint64_t>(changed.tv_sec) * 1000000000 + changed.tv_nsec};
}

int changed()
{
  LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file SQL input changed during query");
  return OB_INVALID_ARGUMENT;
}

int skip_bom(Input &input)
{
  char bom[3];
  int ret = OB_SUCCESS;
  if (input.stamp().size >= sizeof(bom)) {
    if (OB_FAIL(input.read_at(0, bom, sizeof(bom)))) {
    } else if (memcmp(bom, "\xef\xbb\xbf", 3) == 0) {
      input.rewind(3);
    }
  }
  return ret;
}

int csv_record(Input &input, const Options &opts, uint64_t row, std::vector<Cell> &cells)
{
  enum class State { START, TEXT, QUOTED, CLOSED };
  int ret = OB_SUCCESS;
  State state = State::START;
  bool quoted = false, done = false;
  std::string field;
  uint64_t start = input.offset();
  cells.clear();
  while (OB_SUCC(ret) && !done) {
    int c = -1;
    if (OB_FAIL(input.byte(c))) {
    } else if (input.offset() - start > MAX_RECORD || field.size() > MAX_FIELD) {
      ret = invalid(row, cells.size() + 1, input.offset(), "record or field exceeds size limit");
    } else if (state == State::QUOTED) {
      if (c < 0) {
        ret = invalid(row, cells.size() + 1, input.offset(), "unterminated quoted field");
      } else if (c == opts.quote) {
        state = State::CLOSED;
      } else if (c == opts.escape && opts.escape != opts.quote) {
        int escaped = -1;
        if (OB_FAIL(input.byte(escaped))) {
        } else if (escaped != opts.quote && escaped != opts.escape) {
          ret = invalid(row, cells.size() + 1, input.offset(), "invalid escape");
        } else {
          field.push_back(static_cast<char>(escaped));
        }
      } else {
        field.push_back(static_cast<char>(c));
      }
    } else if (state == State::CLOSED && c == opts.quote && opts.escape == opts.quote) {
      field.push_back(opts.quote);
      state = State::QUOTED;
    } else if (c < 0 || c == opts.delimiter || c == '\n' || c == '\r') {
      if (c < 0 && input.offset() == start) {
        ret = OB_ITER_END;
      } else if (cells.size() == MAX_COLUMNS || !utf8(field)) {
        ret = invalid(row, cells.size() + 1, input.offset(), "too many fields or invalid UTF-8");
      } else {
        Type type = !quoted && (field.empty() || field == opts.null_text)
            ? Type::NIL : classify(field, true);
        cells.push_back({type, std::move(field)});
        field.clear();
        quoted = false;
        state = State::START;
        done = c != opts.delimiter;
        if (c == '\r') {
          const uint64_t after_cr = input.offset();
          int next = -1;
          if (OB_FAIL(input.byte(next))) {
          } else if (next != '\n' && next >= 0) {
            input.seek(after_cr);
          }
        }
      }
    } else if (state == State::START && c == opts.quote) {
      quoted = true;
      state = State::QUOTED;
    } else if (state == State::CLOSED || c == opts.quote) {
      ret = invalid(row, cells.size() + 1, input.offset(), "unexpected quote or trailing character");
    } else {
      state = State::TEXT;
      field.push_back(static_cast<char>(c));
    }
  }
  return ret;
}

void append_codepoint(std::string &out, unsigned cp)
{
  if (cp < 0x80) {
    out.push_back(static_cast<char>(cp));
  } else {
    unsigned n = cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
    out.push_back(static_cast<char>((0xff << (8 - n)) | (cp >> (6 * (n - 1)))));
    while (--n > 0) {
      out.push_back(static_cast<char>(0x80 | ((cp >> (6 * (n - 1))) & 0x3f)));
    }
  }
}

// Preserve the order of keys and the exact lexeme of each JSON number.
class JsonLine
{
public:
  explicit JsonLine(std::string_view line) : line_(line) {}
  int parse(std::vector<std::string> &keys, std::vector<Cell> &cells)
  {
    if (!take('{')) { return peek() == '[' ? OB_NOT_SUPPORTED : OB_INVALID_ARGUMENT; }
    if (take('}')) { return peek() == 0 ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
    do {
      std::string key;
      Cell cell;
      if (keys.size() == MAX_COLUMNS || !string(key) || !take(':') ||
          std::find(keys.begin(), keys.end(), key) != keys.end()) {
        return OB_INVALID_ARGUMENT;
      }
      char first = peek();
      if (first == '{' || first == '[') {
        return OB_NOT_SUPPORTED;
      } else if (first == '"') {
        if (!string(cell.text)) { return OB_INVALID_ARGUMENT; }
        cell.type = classify(cell.text, false);
      } else {
        size_t start = pos_;
        while (pos_ < line_.size() && strchr(" \t\r\n,}", line_[pos_]) == nullptr) { ++pos_; }
        std::string_view value = line_.substr(start, pos_ - start);
        if (value == "true" || value == "false") {
          cell.type = Type::BOOL;
        } else if (value != "null") {
          if (value.size() > MAX_FIELD || !number(value)) { return OB_INVALID_ARGUMENT; }
          cell.type = classify(value, true);
        }
        cell.text.assign(value);
      }
      keys.push_back(std::move(key));
      cells.push_back(std::move(cell));
    } while (take(','));
    return take('}') && peek() == 0 ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  }

  size_t pos() const { return pos_; }
private:
  char peek()
  {
    while (pos_ < line_.size() && (line_[pos_] == ' ' || line_[pos_] == '\t' || line_[pos_] == '\r')) {
      ++pos_;
    }
    return pos_ < line_.size() ? line_[pos_] : 0;
  }
  bool take(char c)
  {
    if (peek() != c) { return false; }
    ++pos_;
    return true;
  }
  bool hex(unsigned &v)
  {
    v = 0;
    if (line_.size() - pos_ < 4) { return false; }
    for (int i = 0; i < 4; ++i) {
      char c = line_[pos_++];
      unsigned d = digit(c) ? c - '0' : (c | 32) >= 'a' && (c | 32) <= 'f' ? (c | 32) - 'a' + 10 : 16;
      if (d == 16) { return false; }
      v = v * 16 + d;
    }
    return true;
  }
  bool string(std::string &out)
  {
    if (!take('"')) { return false; }
    while (pos_ < line_.size() && out.size() <= MAX_FIELD) {
      unsigned char c = line_[pos_++];
      if (c == '"') { return utf8(out); }
      if (c < 32) { return false; }
      if (c != '\\') {
        out.push_back(c);
      } else if (pos_ == line_.size()) {
        return false;
      } else {
        c = line_[pos_++];
        const char *escapes = "\"\\/bfnrt";
        const char *found = strchr(escapes, c);
        if (c && found) {
          out.push_back("\"\\/\b\f\n\r\t"[found - escapes]);
        } else if (c == 'u') {
          unsigned cp = 0;
          if (!hex(cp)) { return false; }
          if (cp >= 0xd800 && cp <= 0xdbff) {
            if (line_.substr(pos_, 2) != "\\u") { return false; }
            pos_ += 2;
            unsigned low = 0;
            if (!hex(low) || low < 0xdc00 || low > 0xdfff) { return false; }
            cp = 0x10000 + ((cp - 0xd800) << 10) + low - 0xdc00;
          } else if (cp >= 0xdc00 && cp <= 0xdfff) {
            return false;
          }
          append_codepoint(out, cp);
        } else {
          return false;
        }
      }
    }
    return false;
  }
  static bool number(std::string_view s)
  {
    size_t i = 0;
    if (i < s.size() && s[i] == '-') { ++i; }
    if (i == s.size() || !digit(s[i])) { return false; }
    if (s[i++] != '0') {
      while (i < s.size() && digit(s[i])) { ++i; }
    }
    if (i < s.size() && s[i] == '.') {
      size_t first = ++i;
      while (i < s.size() && digit(s[i])) { ++i; }
      if (i == first) { return false; }
    }
    if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
      ++i;
      if (i < s.size() && (s[i] == '-' || s[i] == '+')) { ++i; }
      size_t first = i;
      while (i < s.size() && digit(s[i])) { ++i; }
      if (i == first) { return false; }
    }
    return i == s.size();
  }
  std::string_view line_;
  size_t pos_ = 0;
};

int json_record(Input &input, uint64_t &line_no, std::vector<std::string> &keys,
                std::vector<Cell> &cells)
{
  int ret = OB_SUCCESS;
  keys.clear();
  cells.clear();
  std::string line;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int c = -1;
    uint64_t start = input.offset();
    line.clear();
    ++line_no;
    while (OB_SUCC(ret) && OB_SUCC(input.byte(c)) && c >= 0 && c != '\n') {
      if (line.size() == MAX_RECORD || c == 0) {
        ret = invalid(line_no, 1, input.offset(), "invalid or oversized JSONL record");
      } else {
        line.push_back(c);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (line.find_first_not_of(" \t\r") != std::string::npos) {
      JsonLine parser(line);
      if (OB_FAIL(parser.parse(keys, cells))) {
        if (ret == OB_NOT_SUPPORTED) {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL JSONL nested values");
        } else {
          ret = invalid(line_no, keys.size() + 1, start + parser.pos(), "invalid JSONL object");
        }
      }
      done = true;
    } else if (c < 0) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int unique_name(Plan &plan, std::string name, Type type, bool nullable)
{
  if (plan.columns.size() == MAX_COLUMNS || name.size() > 256 || !utf8(name) ||
      name.find('\0') != std::string::npos) {
    return OB_INVALID_ARGUMENT;
  }
  if (name.empty()) { name = "column" + std::to_string(plan.columns.size()); }
  std::string candidate = name;
  size_t suffix = 2;
  auto used = [&](const Column &col) { return view(col.name).case_compare(view(candidate)) == 0; };
  while (std::any_of(plan.columns.begin(), plan.columns.end(), used)) {
    candidate = name + "__" + std::to_string(suffix++);
  }
  plan.columns.push_back({std::move(candidate), type, nullable, 0});
  return OB_SUCCESS;
}

void merge_cell(Column &col, const Cell &cell)
{
  col.nullable |= cell.type == Type::NIL;
  col.type = merge(col.type, cell.type);
  if (cell.type == Type::DATETIME) {
    col.scale = std::max<int16_t>(col.scale, cell.text.size() > 19 ? cell.text.size() - 20 : 0);
  }
}

int infer_text(Plan &plan, Input &input)
{
  int ret = skip_bom(input);
  std::vector<Cell> cells;
  std::vector<std::string> keys;
  uint64_t line = 0;
  bool first = true;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
    } else if (plan.format == Format::CSV) {
      ret = csv_record(input, plan.options, ++line, cells);
    } else {
      ret = json_record(input, line, keys, cells);
    }
    if (OB_FAIL(ret)) {
    } else if (plan.format == Format::CSV) {
      if (first) {
        for (size_t i = 0; OB_SUCC(ret) && i < cells.size(); ++i) {
          ret = unique_name(plan, plan.options.header ? cells[i].text : "column" + std::to_string(i),
                            Type::NIL, false);
        }
      }
      if (OB_SUCC(ret) && !(first && plan.options.header)) {
        if (cells.size() != plan.columns.size()) {
          ret = invalid(line, cells.size() + 1, input.offset(), "CSV field count differs from schema");
        } else {
          for (size_t i = 0; i < cells.size(); ++i) { merge_cell(plan.columns[i], cells[i]); }
          ++plan.row_count;
        }
      }
    } else {
      std::vector<bool> present(plan.columns.size(), false);
      for (size_t i = 0; OB_SUCC(ret) && i < keys.size(); ++i) {
        auto col = std::find_if(plan.columns.begin(), plan.columns.end(),
            [&](const Column &c) { return view(c.name).case_compare(view(keys[i])) == 0; });
        if (col == plan.columns.end()) {
          if (keys[i].empty() || OB_FAIL(unique_name(plan, keys[i], Type::NIL, plan.row_count != 0))) {
            ret = OB_INVALID_ARGUMENT;
          } else {
            present.push_back(true);
            merge_cell(plan.columns.back(), cells[i]);
          }
        } else {
          const size_t idx = col - plan.columns.begin();
          if (present[idx] || col->name != keys[i]) {
            ret = invalid(line, i + 1, input.offset(), "ambiguous JSONL field name");
          } else {
            present[idx] = true;
            merge_cell(*col, cells[i]);
          }
        }
      }
      for (size_t i = 0; i < present.size(); ++i) { plan.columns[i].nullable |= !present[i]; }
      ++plan.row_count;
    }
    first = false;
  }
  if (ret == OB_ITER_END) { ret = input.check(); }
  if (OB_SUCC(ret) && plan.columns.empty()) {
    ret = invalid(line, 1, 0, "cannot infer a schema without columns");
  }
  for (Column &c : plan.columns) {
    if (c.type == Type::NIL) { c.type = Type::TEXT; c.nullable = true; }
  }
  return ret;
}


int infer_glob(ObString secure, Plan &plan)
{
  if (plan.path.size() >= PATH_MAX || plan.path.find('\0') != plan.path.npos) {
    return OB_INVALID_ARGUMENT;
  }
  size_t slash = plan.path.find_last_of('/');
  std::string parent = slash == std::string::npos ? "." : plan.path.substr(0, std::max<size_t>(1, slash));
  std::string name = slash == std::string::npos ? plan.path : plan.path.substr(slash + 1);
  std::string canonical;
  int fd = -1;
  int ret = OB_SUCCESS;
  if (name.empty() || parent.find_first_of("*?[") != std::string::npos) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(canonical_path(secure, parent, canonical))) {
  } else if (OB_FAIL(directory_fd(canonical, fd))) {
  } else {
    DIR *dir = ::fdopendir(fd);
    if (!dir) {
      ::close(fd);
      ret = OB_IO_ERROR;
    } else {
      size_t bytes = 0;
      for (dirent *ent = nullptr; OB_SUCC(ret) && (ent = ::readdir(dir));) {
        if (OB_FAIL(THIS_WORKER.check_status())) { break; }
        if (fnmatch(name.c_str(), ent->d_name, FNM_PERIOD) != 0) { continue; }
        struct stat st{};
        if (::fstatat(fd, ent->d_name, &st, 0) != 0 || !S_ISREG(st.st_mode)) { continue; }
        std::string match;
        std::string path = canonical == "/" ? canonical + ent->d_name : canonical + "/" + ent->d_name;
        if (OB_FAIL(canonical_path(secure, path, match))) {
        } else if (plan.matches.size() == MAX_MATCHES || (bytes += match.size() + 8) > MAX_PLAN / 2) {
          ret = OB_SIZE_OVERFLOW;
        } else {
          plan.matches.push_back(std::move(match));
        }
      }
      ::closedir(dir);
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(plan.matches.begin(), plan.matches.end());
    plan.matches.erase(std::unique(plan.matches.begin(), plan.matches.end()), plan.matches.end());
    plan.columns.push_back({"file", Type::TEXT, false, 0});
    plan.row_count = plan.matches.size();
  }
  return ret;
}

void put(std::string &buf, uint64_t v)
{
  for (int i = 0; i < 8; ++i) { buf.push_back(static_cast<char>(v >> (i * 8))); }
}

void put(std::string &buf, const std::string &s)
{
  put(buf, s.size());
  buf.append(s);
}

class PlanReader
{
public:
  explicit PlanReader(ObString data) : data_(data.ptr(), data.length()) {}
  uint64_t number()
  {
    if (data_.size() < 8) { ok_ = false; return 0; }
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) { value |= uint64_t(uint8_t(data_[i])) << (8 * i); }
    data_.remove_prefix(8);
    return value;
  }
  std::string string()
  {
    uint64_t len = number();
    if (len > data_.size()) { ok_ = false; return {}; }
    std::string result(data_.substr(0, len));
    data_.remove_prefix(len);
    return result;
  }
  bool complete() const { return ok_ && data_.empty(); }
private:
  std::string_view data_;
  bool ok_ = true;
};

} // namespace

const char *type_name(Type type)
{
  switch (type) {
    case Type::INT: return "BIGINT";
    case Type::DOUBLE: return "DOUBLE";
    case Type::BOOL: return "BOOLEAN";
    case Type::DATE: return "DATE";
    case Type::DATETIME: return "DATETIME";
    default: return "VARCHAR";
  }
}

Type merge(Type lhs, Type rhs)
{
  if (lhs == Type::NIL || lhs == rhs) { return rhs; }
  if (rhs == Type::NIL) { return lhs; }
  if ((lhs == Type::INT && rhs == Type::DOUBLE) || (rhs == Type::INT && lhs == Type::DOUBLE)) {
    return Type::DOUBLE;
  }
  if ((lhs == Type::DATE && rhs == Type::DATETIME) || (rhs == Type::DATE && lhs == Type::DATETIME)) {
    return Type::DATETIME;
  }
  return Type::TEXT;
}

bool integer(std::string_view s, int64_t &value)
{
  if (!s.empty() && s[0] == '+') {
    s.remove_prefix(1);
    if (s.empty() || !digit(s[0])) { return false; }
  }
  if (s.empty()) { return false; }
  const auto result = std::from_chars(s.data(), s.data() + s.size(), value);
  return result.ec == std::errc{} && result.ptr == s.data() + s.size();
}

bool real(std::string_view s, double &value)
{
  if (!s.empty() && s[0] == '+') {
    s.remove_prefix(1);
    if (s.empty() || s[0] == '-' || s[0] == '+') { return false; }
  }
  if (s.empty()) { return false; }
  const auto result = std::from_chars(s.data(), s.data() + s.size(), value);
  return result.ec == std::errc{} && result.ptr == s.data() + s.size() && std::isfinite(value);
}

bool calendar(std::string_view s, int64_t &micros, int16_t &scale)
{
  if (s.size() != 10 && (s.size() < 19 || s.size() > 26)) { return false; }
  if (s[4] != '-' || s[7] != '-') { return false; }
  int y = digits(s, 0, 4), m = digits(s, 5, 2), d = digits(s, 8, 2);
  static constexpr int days[] = {31,28,31,30,31,30,31,31,30,31,30,31};
  if (y <= 0 || m < 1 || m > 12 || d < 1 ||
      d > days[m - 1] + (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0))) {
    return false;
  }
  // Gregorian civil date to days since 1970-01-01.
  y -= m <= 2;
  const int era = y / 400, yoe = y % 400;
  const int doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  micros = (era * 146097LL + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468) * MICROS_PER_DAY;
  scale = 0;
  if (s.size() > 10) {
    if ((s[10] != ' ' && s[10] != 'T') || s[13] != ':' || s[16] != ':') { return false; }
    int h = digits(s, 11, 2), min = digits(s, 14, 2), sec = digits(s, 17, 2);
    if (h < 0 || h > 23 || min < 0 || min > 59 || sec < 0 || sec > 59) { return false; }
    micros += (h * 3600LL + min * 60 + sec) * 1000000;
    if (s.size() > 19) {
      if (s[19] != '.' || s.size() == 20) { return false; }
      scale = s.size() - 20;
      int fraction = digits(s, 20, scale);
      if (fraction < 0) { return false; }
      for (int i = scale; i < 6; ++i) { fraction *= 10; }
      micros += fraction;
    }
  }
  return true;
}

Type classify(std::string_view s, bool infer_number)
{
  int64_t value = 0;
  double fp = 0;
  int16_t scale = 0;
  if (infer_number) {
    if (view(s).case_compare("true") == 0 || view(s).case_compare("false") == 0) { return Type::BOOL; }
    if (integer(s, value)) { return Type::INT; }
    if (s.find_first_of(".eE") != s.npos && real(s, fp)) { return Type::DOUBLE; }
  }
  if (calendar(s, value, scale)) { return s.size() == 10 ? Type::DATE : Type::DATETIME; }
  return Type::TEXT;
}

bool utf8(std::string_view s)
{
  size_t i = 0;
  while (i < s.size()) {
    unsigned c = static_cast<unsigned char>(s[i++]);
    if (c < 0x80) { continue; }
    unsigned n = c >= 0xc2 && c <= 0xdf ? 1 : c >= 0xe0 && c <= 0xef ? 2 : c >= 0xf0 && c <= 0xf4 ? 3 : 0;
    if (n == 0 || n > s.size() - i) { return false; }
    unsigned b = static_cast<unsigned char>(s[i]);
    if ((c == 0xe0 && b < 0xa0) || (c == 0xed && b >= 0xa0) ||
        (c == 0xf0 && b < 0x90) || (c == 0xf4 && b >= 0x90)) { return false; }
    while (n-- > 0) {
      if ((static_cast<unsigned char>(s[i++]) & 0xc0) != 0x80) { return false; }
    }
  }
  return true;
}

int canonical_path(ObString secure, const std::string &path, std::string &canonical)
{
  int ret = OB_SUCCESS;
  char resolved[PATH_MAX], allowed[PATH_MAX];
  struct stat st{};
  if (path.empty() || path.size() >= PATH_MAX || path.find('\0') != std::string::npos) {
    ret = OB_INVALID_ARGUMENT;
  } else if (secure.empty() || secure.case_compare("NULL") == 0 || secure.length() >= PATH_MAX ||
             memchr(secure.ptr(), '\0', secure.length()) != nullptr) {
    ret = OB_ERR_NO_PRIVILEGE;
  } else {
    std::string base(secure.ptr(), secure.length());
    if (!::realpath(base.c_str(), allowed) || ::stat(allowed, &st) != 0 || !S_ISDIR(st.st_mode)) {
      ret = OB_ERR_NO_PRIVILEGE;
    } else if (!::realpath(path.c_str(), resolved)) {
      ret = errno == ENOENT || errno == ENOTDIR ? OB_FILE_NOT_EXIST : OB_ERR_NO_PRIVILEGE;
    } else {
      std::string_view root(allowed), candidate(resolved);
      if (root != "/" && candidate != root &&
          (candidate.size() <= root.size() || candidate.substr(0, root.size()) != root ||
           candidate[root.size()] != '/')) {
        ret = OB_ERR_NO_PRIVILEGE;
      } else {
        canonical.assign(candidate);
      }
    }
  }
  if (ret == OB_ERR_NO_PRIVILEGE) {
    LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "file SQL path outside secure_file_priv");
  }
  return ret;
}

int directory_fd(const std::string &path, int &fd)
{
  fd = -1;
  if (path.empty() || path.front() != '/') { return OB_INVALID_ARGUMENT; }
  fd = ::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  size_t start = 1;
  while (fd >= 0 && start < path.size()) {
    size_t end = path.find('/', start);
    std::string part = path.substr(start, end == std::string::npos ? end : end - start);
    if (part.empty() || part == "." || part == "..") { ::close(fd); fd = -1; break; }
    int next = ::openat(fd, part.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    ::close(fd);
    fd = next;
    if (end == std::string::npos) { break; }
    start = end + 1;
  }
  return fd < 0 ? OB_ERR_NO_PRIVILEGE : OB_SUCCESS;
}

Input::~Input()
{
  if (fd_ >= 0) { ::close(fd_); }
}

int Input::open(const std::string &path, const Stamp *expected)
{
  int ret = OB_SUCCESS;
  size_t slash = path.find_last_of('/');
  int parent = -1;
  struct stat st{};
  if (fd_ >= 0 || slash == std::string::npos) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(directory_fd(path.substr(0, std::max<size_t>(1, slash)), parent))) {
  } else if (::fstatat(parent, path.c_str() + slash + 1, &st, AT_SYMLINK_NOFOLLOW) != 0) {
    ret = errno == ENOENT ? OB_FILE_NOT_EXIST : OB_ERR_NO_PRIVILEGE;
    ::close(parent);
  } else if (!S_ISREG(st.st_mode)) {
    ret = OB_INVALID_ARGUMENT;
    ::close(parent);
  } else {
    fd_ = ::openat(parent, path.c_str() + slash + 1, O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW);
    int saved_errno = errno;
    ::close(parent);
    if (fd_ < 0) {
      ret = saved_errno == ENOENT ? OB_FILE_NOT_EXIST : OB_ERR_NO_PRIVILEGE;
    } else if (::fstat(fd_, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      stamp_ = file_stamp(st);
      if (expected && !(*expected == stamp_)) { ret = changed(); }
    }
  }
  if (OB_FAIL(ret) && fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  return ret;
}

int Input::check() const
{
  struct stat st{};
  return fd_ >= 0 && ::fstat(fd_, &st) == 0 && file_stamp(st) == stamp_ ? OB_SUCCESS : changed();
}

int Input::read_at(uint64_t offset, void *data, size_t count) const
{
  int ret = OB_SUCCESS;
  if (offset > stamp_.size || count > stamp_.size - offset) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(check())) {
  } else {
    char *dst = static_cast<char *>(data);
    while (count > 0 && OB_SUCC(ret)) {
      ssize_t n = ::pread(fd_, dst, count, offset);
      if (n < 0 && errno == EINTR) { continue; }
      if (n <= 0) { ret = changed(); }
      else { dst += n; offset += n; count -= n; }
    }
    if (OB_SUCC(ret)) { ret = check(); }
  }
  return ret;
}

int Input::byte(int &value)
{
  int ret = OB_SUCCESS;
  value = -1;
  if (offset_ < stamp_.size) {
    if (offset_ < buffer_start_ || offset_ >= buffer_start_ + buffer_size_) {
      buffer_start_ = offset_;
      buffer_size_ = std::min<uint64_t>(sizeof(buffer_), stamp_.size - offset_);
      if (OB_FAIL(read_at(buffer_start_, buffer_, buffer_size_))) { buffer_size_ = 0; }
    }
    if (OB_SUCC(ret)) { value = static_cast<unsigned char>(buffer_[offset_++ - buffer_start_]); }
  }
  return ret;
}


int Plan::encode(ObIAllocator &allocator, ObString &result) const
{
  std::string bytes;
  put(bytes, 1);
  put(bytes, static_cast<uint8_t>(format));
  put(bytes, options.header);
  put(bytes, uint8_t(options.delimiter));
  put(bytes, uint8_t(options.quote));
  put(bytes, uint8_t(options.escape));
  put(bytes, options.null_text);
  put(bytes, path);
  put(bytes, stamp.device); put(bytes, stamp.inode); put(bytes, stamp.size);
  put(bytes, stamp.modified); put(bytes, stamp.changed); put(bytes, row_count);
  put(bytes, columns.size());
  for (const Column &c : columns) {
    put(bytes, c.name); put(bytes, static_cast<uint8_t>(c.type)); put(bytes, c.nullable); put(bytes, c.scale);
  }
  put(bytes, matches.size());
  for (const auto &match : matches) { put(bytes, match); }
  return bytes.size() <= MAX_PLAN ? ob_write_string(allocator, view(bytes), result) : OB_SIZE_OVERFLOW;
}

int Plan::decode(ObString encoded)
{
  if (encoded.empty() || encoded.length() > MAX_PLAN) { return OB_INVALID_ARGUMENT; }
  PlanReader r(encoded);
  if (r.number() != 1) { return OB_INVALID_ARGUMENT; }
  const uint64_t fmt = r.number();
  if (fmt > static_cast<uint8_t>(Format::GLOB)) { return OB_INVALID_ARGUMENT; }
  format = static_cast<Format>(fmt);
  options.header = r.number();
  options.delimiter = r.number(); options.quote = r.number(); options.escape = r.number();
  options.null_text = r.string();
  path = r.string();
  stamp.device = r.number(); stamp.inode = r.number(); stamp.size = r.number();
  stamp.modified = r.number(); stamp.changed = r.number(); row_count = r.number();
  uint64_t count = r.number();
  if (count > MAX_COLUMNS) { return OB_INVALID_ARGUMENT; }
  for (uint64_t i = 0; i < count; ++i) {
    Column c;
    c.name = r.string();
    uint64_t kind = r.number();
    if (kind > static_cast<uint8_t>(Type::DATETIME)) { return OB_INVALID_ARGUMENT; }
    c.type = static_cast<Type>(kind); c.nullable = r.number(); c.scale = r.number();
    columns.push_back(std::move(c));
  }
  count = r.number();
  if (count > MAX_MATCHES) { return OB_INVALID_ARGUMENT; }
  for (uint64_t i = 0; i < count; ++i) { matches.push_back(r.string()); }
  return r.complete() ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}

int infer(ObString secure, Plan &plan)
{
  if (plan.format == Format::GLOB) { return infer_glob(secure, plan); }
  int ret = OB_SUCCESS;
  std::string path;
  std::unique_ptr<Input> input(new (std::nothrow) Input);
  if (!input) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(canonical_path(secure, plan.path, path))) {
  } else if (OB_FAIL(input->open(path, nullptr))) {
  } else {
    plan.path = std::move(path);
    plan.stamp = input->stamp();
    if (plan.format == Format::PARQUET) {
      Parquet parquet(*input);
      ret = parquet.schema(plan);
    } else {
      ret = infer_text(plan, *input);
    }
  }
  return ret;
}

Scanner::~Scanner() { delete parquet_; }

int Scanner::open(ObString secure, ObString encoded, const std::vector<int> &projection)
{
  int ret = OB_SUCCESS;
  std::string path;
  if (OB_FAIL(plan_.decode(encoded))) { return ret; }
  for (int col : projection) {
    if (col < 0 || static_cast<size_t>(col) >= plan_.columns.size()) { return OB_INVALID_ARGUMENT; }
  }
  if (plan_.format == Format::GLOB) {
    for (const auto &match : plan_.matches) {
      if (OB_FAIL(canonical_path(secure, match, path))) { break; }
      if (path != match) { ret = changed(); break; }
    }
  } else if (OB_FAIL(canonical_path(secure, plan_.path, path))) {
  } else if (path != plan_.path) {
    ret = changed();
  } else if (OB_FAIL(input_.open(path, &plan_.stamp))) {
  } else if (plan_.format == Format::PARQUET) {
    parquet_ = new (std::nothrow) Parquet(input_);
    if (!parquet_) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else if (OB_FAIL(parquet_->open(plan_, projection))) {}
  }
  if (OB_SUCC(ret)) {
    projection_ = projection;
    ret = rescan();
  }
  return ret;
}

int Scanner::rescan()
{
  int ret = OB_SUCCESS;
  row_ = 0;
  if (plan_.format == Format::GLOB) {
  } else if (OB_FAIL(input_.check())) {
  } else if (parquet_) {
    ret = parquet_->rescan();
  } else {
    input_.rewind();
    if (OB_FAIL(skip_bom(input_))) {
    } else if (plan_.format == Format::CSV && plan_.options.header) {
      ret = csv_record(input_, plan_.options, ++row_, record_);
    }
  }
  return ret;
}

int Scanner::next(std::vector<Cell> &cells)
{
  int ret = OB_SUCCESS;
  cells.clear();
  if (plan_.format == Format::GLOB) {
    if (row_ == plan_.matches.size()) { ret = OB_ITER_END; }
    else {
      for (int idx : projection_) {
        (void)idx;
        cells.push_back({Type::TEXT, plan_.matches[row_]});
      }
      ++row_;
    }
  } else if (parquet_) {
    ret = parquet_->next(cells);
  } else if (plan_.format == Format::CSV) {
    if (OB_FAIL(csv_record(input_, plan_.options, ++row_, record_))) {
    } else if (record_.size() != plan_.columns.size()) {
      ret = invalid(row_, record_.size() + 1, input_.offset(), "CSV field count differs from schema");
    } else {
      for (int idx : projection_) { cells.push_back(record_[idx]); }
    }
  } else if (OB_FAIL(json_record(input_, row_, keys_, record_))) {
  } else {
    for (int idx : projection_) {
      auto key = std::find(keys_.begin(), keys_.end(), plan_.columns[idx].name);
      cells.push_back(key == keys_.end() ? Cell{} : record_[key - keys_.begin()]);
    }
  }
  if (ret == OB_ITER_END && plan_.format != Format::GLOB && input_.check() != OB_SUCCESS) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

} // namespace oceanbase::sql::file_sql


namespace oceanbase::sql::file_sql {
using namespace common;

int copy_target(ObString secure, std::string &target)
{
  int ret = OB_SUCCESS;
  size_t slash = target.find_last_of('/');
  std::string basename = slash == target.npos ? target : target.substr(slash + 1);
  std::string parent = slash == target.npos ? "." : target.substr(0, std::max<size_t>(1, slash));
  std::string checked;
  if (basename.empty() || basename == "." || basename == ".." || basename.size() > NAME_MAX ||
      target.find('\0') != target.npos) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(canonical_path(secure, parent, checked))) {
  } else {
    target = checked == "/" ? checked + basename : checked + "/" + basename;
  }
  return ret;
}

CsvOutput::~CsvOutput()
{
  if (file_ >= 0) { ::close(file_); }
  if (parent_ >= 0) {
    if (!temp_.empty()) { ::unlinkat(parent_, temp_.c_str(), 0); }
    ::close(parent_);
  }
}

int CsvOutput::open(ObString secure, const Plan &plan)
{
  int ret = OB_SUCCESS;
  std::string target = plan.path;
  struct stat st{};
  if (OB_FAIL(copy_target(secure, target))) {
  } else if (target != plan.path) {
    ret = OB_ERR_NO_PRIVILEGE;
  } else {
    const size_t slash = target.find_last_of('/');
    basename_ = target.substr(slash + 1);
    options_ = plan.options;
    if (OB_FAIL(directory_fd(target.substr(0, std::max<size_t>(1, slash)), parent_))) {
    } else if (::fstatat(parent_, basename_.c_str(), &st, AT_SYMLINK_NOFOLLOW) == 0) {
      ret = OB_FILE_ALREADY_EXIST;
    } else if (errno != ENOENT) {
      ret = OB_IO_ERROR;
    } else {
      static std::atomic<uint64_t> sequence{0};
      for (int tries = 0; file_ < 0 && tries < 32; ++tries) {
        std::string candidate = ".seekdb-copy-" + std::to_string(::getpid()) + "-"
            + std::to_string(sequence.fetch_add(1, std::memory_order_relaxed));
        file_ = ::openat(parent_, candidate.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (file_ >= 0) {
          temp_ = std::move(candidate);
        } else if (errno != EEXIST) {
          break;
        }
      }
      if (file_ < 0) { ret = OB_IO_ERROR; }
    }
  }
  if (OB_SUCC(ret) && options_.header) {
    std::vector<Cell> header;
    for (const auto &column : plan.columns) { header.push_back({Type::TEXT, column.name}); }
    ret = write(header);
  }
  return ret;
}

int CsvOutput::write(const std::vector<Cell> &cells)
{
  std::string record;
  for (size_t i = 0; i < cells.size(); ++i) {
    if (i > 0) { record.push_back(options_.delimiter); }
    const Cell &cell = cells[i];
    const bool is_null = cell.type == Type::NIL;
    const std::string &text = is_null ? options_.null_text : cell.text;
    if (text.size() > MAX_FIELD || record.size() + text.size() > MAX_RECORD || !utf8(text)) {
      return OB_INVALID_ARGUMENT;
    }
    const bool quoted = !is_null && (cell.type == Type::TEXT || text == options_.null_text ||
        text.find(options_.delimiter) != text.npos || text.find(options_.quote) != text.npos ||
        text.find_first_of("\r\n") != text.npos);
    if (quoted) {
      record.push_back(options_.quote);
      for (char c : text) {
        if (c == options_.quote || c == options_.escape) { record.push_back(options_.escape); }
        record.push_back(c);
      }
      record.push_back(options_.quote);
    } else {
      record.append(text);
    }
    if (record.size() >= MAX_RECORD) { return OB_SIZE_OVERFLOW; }
  }
  record.push_back('\n');
  size_t done = 0;
  while (done < record.size()) {
    ssize_t n = ::write(file_, record.data() + done, record.size() - done);
    if (n < 0 && errno == EINTR) { continue; }
    if (n <= 0) { return OB_IO_ERROR; }
    done += n;
  }
  return OB_SUCCESS;
}

int CsvOutput::publish()
{
  if (::fsync(file_) != 0) { return OB_IO_ERROR; }
  int result = ::close(file_);
  file_ = -1;
  if (result != 0) { return OB_IO_ERROR; }
  // The directory descriptor pins the checked directory across the query.
  // Hard-link publication fails atomically if the destination already exists.
  if (::linkat(parent_, temp_.c_str(), parent_, basename_.c_str(), 0) != 0) {
    return errno == EEXIST ? OB_FILE_ALREADY_EXIST : OB_IO_ERROR;
  }
  if (::unlinkat(parent_, temp_.c_str(), 0) == 0) { temp_.clear(); }
  return OB_SUCCESS;
}

} // namespace oceanbase::sql::file_sql
