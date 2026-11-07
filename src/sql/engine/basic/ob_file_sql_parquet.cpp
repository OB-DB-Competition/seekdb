// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_sql_parquet.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/timezone/ob_time_convert.h"
#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstring>
#include <limits>
#include <new>

namespace oceanbase::sql::file_sql {
using namespace common;
namespace {

constexpr uint64_t MAX_FOOTER = 16 * MAX_FIELD;
constexpr uint64_t MAX_PAGE = 8 * MAX_FIELD;
constexpr uint64_t MAX_PAGE_HEADER = 64 * 1024;
constexpr uint64_t MAX_PAGE_VALUES = 1024 * 1024;
constexpr uint64_t MAX_ACTIVE_BYTES = 128 * MAX_FIELD;
constexpr uint64_t MAX_GROUPS = 65536;
constexpr uint64_t MAX_CHUNKS = 131072;

// Bounds-checked cursor shared by Compact Protocol, page encodings and Snappy.
struct Bytes
{
  std::string_view data;
  size_t pos = 0;
  bool ok = true;

  size_t left() const { return data.size() - pos; }
  std::string_view take(size_t count)
  {
    if (count > left()) { ok = false; return {}; }
    auto result = data.substr(pos, count);
    pos += count;
    return result;
  }
  uint64_t little(unsigned n)
  {
    const auto bytes = take(n);
    uint64_t result = 0;
    for (size_t i = 0; i < bytes.size(); ++i) {
      result |= uint64_t(uint8_t(bytes[i])) << (8 * i);
    }
    return result;
  }
  uint8_t byte() { return little(1); }
  uint64_t varint()
  {
    uint64_t result = 0;
    for (unsigned shift = 0; ok && shift < 64; shift += 7) {
      const auto c = byte();
      if (shift == 63 && c > 1) { ok = false; break; }
      result |= uint64_t(c & 127) << shift;
      if ((c & 128) == 0) { return result; }
    }
    ok = false;
    return 0;
  }
  int64_t zigzag()
  {
    const uint64_t v = varint();
    return static_cast<int64_t>((v >> 1) ^ (0ULL - (v & 1)));
  }
};

enum Compact : uint8_t { STOP, TRUE_VALUE, FALSE_VALUE, BYTE, I16, I32, I64,
                         FLOAT64, BINARY, LIST, SET, MAP, STRUCT };

struct Field { int id; uint8_t type; };

Field field(Bytes &in, int &last)
{
  const uint8_t header = in.byte();
  if (header == 0) { return {0, STOP}; }
  const int64_t id = header >> 4 ? last + (header >> 4) : in.zigzag();
  if (id <= 0 || id > INT16_MAX || (header & 15) == 0 || (header & 15) > STRUCT) {
    in.ok = false;
    return {0, STOP};
  }
  last = static_cast<int>(id);
  return {last, uint8_t(header & 15)};
}

uint64_t list(Bytes &in, uint8_t &type)
{
  uint8_t h = in.byte();
  type = h & 15;
  uint64_t count = (h >> 4) == 15 ? in.varint() : h >> 4;
  if (type == STOP || type > STRUCT || count > in.left()) { in.ok = false; return 0; }
  return count;
}

void skip(Bytes &in, uint8_t type, unsigned depth = 0, bool collection = false)
{
  if (!in.ok || depth > 32) { in.ok = false; return; }
  switch (type) {
    case TRUE_VALUE:
    case FALSE_VALUE:
      if (collection) {
        uint8_t v = in.byte();
        if (v != TRUE_VALUE && v != FALSE_VALUE) { in.ok = false; }
      }
      break;
    case BYTE: in.byte(); break;
    case I16:
    case I32:
    case I64: in.varint(); break;
    case FLOAT64: in.take(8); break;
    case BINARY: in.take(in.varint()); break;
    case LIST:
    case SET: {
      uint8_t elem = STOP;
      uint64_t count = list(in, elem);
      for (uint64_t i = 0; in.ok && i < count; ++i) { skip(in, elem, depth + 1, true); }
      break;
    }
    case MAP: {
      uint64_t count = in.varint();
      if (count > in.left() / 2) { in.ok = false; break; }
      if (count > 0) {
        uint8_t types = in.byte();
        for (uint64_t i = 0; in.ok && i < count; ++i) {
          skip(in, types >> 4, depth + 1, true);
          skip(in, types & 15, depth + 1, true);
        }
      }
      break;
    }
    case STRUCT: {
      int last = 0;
      for (Field f = field(in, last); in.ok && f.type; f = field(in, last)) {
        skip(in, f.type, depth + 1);
      }
      break;
    }
    default: in.ok = false; break;
  }
}

int64_t thrift_int(Bytes &in, Field f)
{
  if (f.type != I16 && f.type != I32 && f.type != I64) { in.ok = false; return -1; }
  return in.zigzag();
}

std::string thrift_string(Bytes &in, uint8_t type)
{
  if (type != BINARY) { in.ok = false; return {}; }
  uint64_t length = in.varint();
  if (length > MAX_FIELD) { in.ok = false; return {}; }
  return std::string(in.take(length));
}

struct Schema
{
  int64_t physical = -1, repetition = -1, children = 0, converted = -1;
  int logical = 0, time_unit = 0;
  bool signed_integer = true;
  std::string name;
  Type type = Type::NIL;
};

int timestamp_unit(Bytes &in)
{
  int unit = 0, last = 0;
  for (Field f = field(in, last); in.ok && f.type; f = field(in, last)) {
    if (f.id == 2 && f.type == STRUCT) {
      int unit_field = 0;
      for (Field u = field(in, unit_field); in.ok && u.type; u = field(in, unit_field)) {
        unit = u.id;
        skip(in, u.type);
      }
    } else {
      skip(in, f.type);
    }
  }
  return unit;
}

Schema schema_element(Bytes &in)
{
  Schema c;
  int last = 0;
  for (Field f = field(in, last); in.ok && f.type; f = field(in, last)) {
    switch (f.id) {
      case 1: c.physical = thrift_int(in, f); break;
      case 3: c.repetition = thrift_int(in, f); break;
      case 4: c.name = thrift_string(in, f.type); break;
      case 5: c.children = thrift_int(in, f); break;
      case 6: c.converted = thrift_int(in, f); break;
      case 10: {
        if (f.type != STRUCT) { in.ok = false; break; }
        int logical_last = 0;
        for (Field l = field(in, logical_last); in.ok && l.type; l = field(in, logical_last)) {
          c.logical = l.id;
          if (l.id == 8 && l.type == STRUCT) {
            c.time_unit = timestamp_unit(in);
          } else if (l.id == 10 && l.type == STRUCT) {
            int int_last = 0;
            for (Field field_info = field(in, int_last); in.ok && field_info.type;
                 field_info = field(in, int_last)) {
              if (field_info.id == 2 && field_info.type == FALSE_VALUE) {
                c.signed_integer = false;
              }
              skip(in, field_info.type);
            }
          } else {
            skip(in, l.type);
          }
        }
        break;
      }
      default: skip(in, f.type); break;
    }
  }
  return c;
}

int resolve_schema(Schema &c)
{
  if (c.children != 0 || c.repetition < 0 || c.repetition > 1) { return OB_NOT_SUPPORTED; }
  if (c.name.empty() || c.name.size() > 256 || !utf8(c.name) || c.name.find('\0') != c.name.npos) {
    return OB_INVALID_ARGUMENT;
  }
  if (c.physical == 1 && (c.logical == 6 || c.converted == 6)) {
    c.type = Type::DATE;
  } else if (c.physical == 2 && (c.logical == 8 || c.converted == 9 || c.converted == 10)) {
    c.type = Type::DATETIME;
    if (!c.time_unit) { c.time_unit = c.converted == 9 ? 1 : c.converted == 10 ? 2 : 0; }
    if (c.time_unit < 1 || c.time_unit > 3) { return OB_NOT_SUPPORTED; }
  } else if (c.physical == 6 && (c.logical == 1 || c.converted == 0 || (c.logical == 0 && c.converted < 0))) {
    c.type = Type::TEXT;
  } else if (c.logical != 0 && (c.logical != 10 || !c.signed_integer)) {
    return OB_NOT_SUPPORTED;
  } else if (c.converted != -1 && (c.converted < 15 || c.converted > 18)) {
    return OB_NOT_SUPPORTED;
  } else {
    switch (c.physical) {
      case 0: c.type = Type::BOOL; break;
      case 1:
      case 2: c.type = Type::INT; break;
      case 4:
      case 5: c.type = Type::DOUBLE; break;
      default: return OB_NOT_SUPPORTED;
    }
  }
  return OB_SUCCESS;
}

struct Chunk
{
  int64_t physical = -1, codec = -1, count = -1, length = -1, data = -1, dictionary = -1;
  bool external = false;
  std::string name;
};
struct Group
{
  int64_t rows = -1;
  std::vector<Chunk> columns;
};

void column_metadata(Bytes &in, Chunk &c)
{
  int last = 0;
  for (Field f = field(in, last); in.ok && f.type; f = field(in, last)) {
    switch (f.id) {
      case 1: c.physical = thrift_int(in, f); break;
      case 3: {
        if (f.type != LIST) { in.ok = false; break; }
        uint8_t t = STOP;
        uint64_t count = list(in, t);
        if (count != 1) { in.ok = false; }
        else { c.name = thrift_string(in, t); }
        break;
      }
      case 4: c.codec = thrift_int(in, f); break;
      case 5: c.count = thrift_int(in, f); break;
      case 7: c.length = thrift_int(in, f); break;
      case 9: c.data = thrift_int(in, f); break;
      case 11: c.dictionary = thrift_int(in, f); break;
      default: skip(in, f.type); break;
    }
  }
}

Chunk column_chunk(Bytes &in)
{
  Chunk c;
  int last = 0;
  for (Field f = field(in, last); in.ok && f.type; f = field(in, last)) {
    if (f.id == 1) {
      c.external = true;
      skip(in, f.type);
    } else if (f.id == 3 && f.type == STRUCT) {
      column_metadata(in, c);
    } else {
      skip(in, f.type);
    }
  }
  return c;
}

Group row_group(Bytes &in)
{
  Group g;
  int last = 0;
  for (Field f = field(in, last); in.ok && f.type; f = field(in, last)) {
    if (f.id == 1 && f.type == LIST) {
      uint8_t t = STOP;
      uint64_t count = list(in, t);
      if (count > MAX_COLUMNS || t != STRUCT) { in.ok = false; }
      for (uint64_t i = 0; in.ok && i < count; ++i) { g.columns.push_back(column_chunk(in)); }
    } else if (f.id == 3) {
      g.rows = thrift_int(in, f);
    } else {
      skip(in, f.type);
    }
  }
  return g;
}

struct Footer
{
  std::vector<Schema> columns;
  std::vector<Group> groups;
  uint64_t rows = 0;
};

int read_footer(Input &input, Footer &footer)
{
  int ret = OB_SUCCESS;
  char magic[4], tail[8];
  const uint64_t size = input.stamp().size;
  if (size < 12) { return OB_INVALID_ARGUMENT; }
  if (OB_FAIL(input.read_at(0, magic, sizeof(magic)))) { return ret; }
  if (OB_FAIL(input.read_at(size - sizeof(tail), tail, sizeof(tail)))) { return ret; }
  if (memcmp(magic, "PAR1", 4) || memcmp(tail + 4, "PAR1", 4)) { return OB_INVALID_ARGUMENT; }
  Bytes trailer{std::string_view(tail, sizeof(tail))};
  uint64_t length = trailer.little(4);
  if (length == 0 || length > MAX_FOOTER || length > size - 12) { return OB_INVALID_ARGUMENT; }
  uint64_t footer_start = size - 8 - length;
  std::string data(length, '\0');
  if (OB_FAIL(input.read_at(footer_start, data.data(), data.size()))) { return ret; }
  Bytes in{data};
  int last = 0;
  int64_t rows = -1, children = -1;
  uint64_t chunks = 0;
  for (Field f = field(in, last); in.ok && f.type; f = field(in, last)) {
    if (f.id == 2 && f.type == LIST) {
      uint8_t t = STOP;
      uint64_t count = list(in, t);
      if (t != STRUCT || count < 2 || count > MAX_COLUMNS + 1) { return OB_NOT_SUPPORTED; }
      Schema root = schema_element(in);
      children = root.children;
      if (children != static_cast<int64_t>(count - 1)) { return OB_NOT_SUPPORTED; }
      for (uint64_t i = 1; in.ok && i < count; ++i) {
        footer.columns.push_back(schema_element(in));
        if (OB_FAIL(resolve_schema(footer.columns.back()))) { return ret; }
      }
    } else if (f.id == 3) {
      rows = thrift_int(in, f);
    } else if (f.id == 4 && f.type == LIST) {
      uint8_t t = STOP;
      uint64_t count = list(in, t);
      if (count > MAX_GROUPS || t != STRUCT) { return OB_INVALID_ARGUMENT; }
      for (uint64_t i = 0; in.ok && i < count; ++i) {
        Group group = row_group(in);
        chunks += group.columns.size();
        if (chunks > MAX_CHUNKS) { return OB_SIZE_OVERFLOW; }
        footer.groups.push_back(std::move(group));
      }
    } else {
      skip(in, f.type);
    }
  }
  if (!in.ok || in.left() != 0 || rows < 0 || children < 1) { return OB_INVALID_ARGUMENT; }
  for (const Group &group : footer.groups) {
    if (group.rows < 0 || uint64_t(group.rows) > uint64_t(rows) - footer.rows ||
        group.columns.size() != footer.columns.size()) {
      return OB_INVALID_ARGUMENT;
    }
    footer.rows += group.rows;
    for (size_t i = 0; i < group.columns.size(); ++i) {
      const Chunk &c = group.columns[i];
      if (c.external || (c.codec != 0 && c.codec != 1)) { return OB_NOT_SUPPORTED; }
      int64_t start = c.dictionary >= 0 ? c.dictionary : c.data;
      if (c.count != group.rows || c.physical != footer.columns[i].physical || c.name != footer.columns[i].name ||
          c.data < start || start < 4 || uint64_t(start) > footer_start || c.length < 0 ||
          uint64_t(c.length) > footer_start - start || c.data - start > c.length) {
        return OB_INVALID_ARGUMENT;
      }
    }
  }
  return uint64_t(rows) == footer.rows ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}


bool decompress_snappy(std::string_view compressed, size_t expected, std::string &out)
{
  Bytes in{compressed};
  if (in.varint() != expected || !in.ok || expected > MAX_PAGE) { return false; }
  out.clear();
  out.reserve(expected);
  while (in.ok && in.left() != 0) {
    unsigned tag = in.byte();
    size_t length = 0, offset = 0;
    switch (tag & 3) {
      case 0: {
        unsigned code = tag >> 2;
        length = (code < 60 ? code : in.little(code - 59)) + 1;
        if (length > expected - out.size()) { return false; }
        const auto literal = in.take(length);
        if (!in.ok) { return false; }
        out.append(literal);
        continue;
      }
      case 1:
        length = 4 + ((tag >> 2) & 7);
        offset = ((tag & 0xe0) << 3) | in.byte();
        break;
      case 2:
        length = 1 + (tag >> 2);
        offset = in.little(2);
        break;
      case 3:
        length = 1 + (tag >> 2);
        offset = in.little(4);
        break;
    }
    if (!in.ok || offset == 0 || offset > out.size() || length > expected - out.size()) {
      return false;
    }
    while (length-- > 0) { out.push_back(out[out.size() - offset]); }
  }
  return in.ok && out.size() == expected;
}

struct PageHeader
{
  int64_t type = -1, compressed = -1, uncompressed = -1, rows = -1, encoding = -1;
  int64_t def_encoding = -1, def_size = 0, rep_size = 0, nulls = -1, v2_rows = -1;
  bool v2_compressed = true;
};

void data_page_header(Bytes &in, int kind, PageHeader &page)
{
  int last = 0;
  for (Field f = field(in, last); in.ok && f.type; f = field(in, last)) {
    if (kind == 8) {
      switch (f.id) {
        case 1: page.rows = thrift_int(in, f); break;
        case 2: page.nulls = thrift_int(in, f); break;
        case 3: page.v2_rows = thrift_int(in, f); break;
        case 4: page.encoding = thrift_int(in, f); break;
        case 5: page.def_size = thrift_int(in, f); break;
        case 6: page.rep_size = thrift_int(in, f); break;
        case 7:
          if (f.type != TRUE_VALUE && f.type != FALSE_VALUE) { in.ok = false; }
          page.v2_compressed = f.type == TRUE_VALUE;
          break;
        default: skip(in, f.type); break;
      }
    } else {
      switch (f.id) {
        case 1: page.rows = thrift_int(in, f); break;
        case 2: page.encoding = thrift_int(in, f); break;
        case 3:
          if (kind == 5) { page.def_encoding = thrift_int(in, f); }
          else { skip(in, f.type); }
          break;
        default: skip(in, f.type); break;
      }
    }
  }
}

PageHeader page_header(Bytes &in)
{
  PageHeader page;
  int last = 0;
  for (Field f = field(in, last); in.ok && f.type; f = field(in, last)) {
    switch (f.id) {
      case 1: page.type = thrift_int(in, f); break;
      case 2: page.uncompressed = thrift_int(in, f); break;
      case 3: page.compressed = thrift_int(in, f); break;
      case 5:
      case 7:
      case 8:
        if (f.type != STRUCT) { in.ok = false; }
        else { data_page_header(in, f.id, page); }
        break;
      default: skip(in, f.type); break;
    }
  }
  return page;
}

// RLE and bit-packed runs are decoded as needed. A run may include up to
// seven padding values, so validate byte bounds separately from row count.
struct Hybrid
{
  Bytes bytes{};
  unsigned width = 0;
  uint64_t remaining = 0;
  bool packed = false;
  uint32_t repeated = 0;
  size_t bit = 0;
  std::string_view run;

  void reset(std::string_view data, unsigned bits)
  {
    *this = Hybrid{};
    bytes.data = data;
    width = bits;
  }
  bool next(uint32_t &value)
  {
    if (width > 32) { return false; }
    if (remaining == 0) {
      uint64_t header = bytes.varint();
      uint64_t count = header >> 1;
      if (!bytes.ok || count == 0 || count > MAX_PAGE_VALUES + 7) { return false; }
      packed = header & 1;
      bit = 0;
      if (packed) {
        run = bytes.take(count * width);
        remaining = count * 8;
      } else {
        repeated = bytes.little((width + 7) / 8);
        remaining = count;
      }
    }
    if (!bytes.ok) { return false; }
    --remaining;
    if (!packed) {
      value = repeated;
    } else {
      uint64_t bits = 0;
      unsigned offset = bit & 7;
      size_t start = bit / 8;
      unsigned count = (offset + width + 7) / 8;
      for (unsigned i = 0; i < count; ++i) { bits |= uint64_t(uint8_t(run[start + i])) << (8 * i); }
      const uint64_t mask = (1ULL << width) - 1;
      value = (bits >> offset) & mask;
      bit += width;
    }
    return true;
  }
};

bool plain_cell(const Schema &col, Bytes &values, uint64_t index, Cell &cell)
{
  cell.type = col.type;
  switch (col.physical) {
    case 0:
      cell.text = (values.byte() & (1U << (index & 7))) ? "true" : "false";
      if ((index & 7) != 7 && values.ok) { --values.pos; }
      break;
    case 1:
    case 2: {
      int64_t n = col.physical == 1 ? int64_t(static_cast<int32_t>(values.little(4)))
                                   : static_cast<int64_t>(values.little(8));
      if (col.type == Type::DATE || col.type == Type::DATETIME) {
        int64_t factor = col.type == Type::DATE ? MICROS_PER_DAY : col.time_unit == 1 ? 1000 : 1;
        if (col.type == Type::DATETIME && col.time_unit == 3) { n /= 1000; }
        if (n > INT64_MAX / factor || n < INT64_MIN / factor) { return false; }
        int64_t micros = n * factor;
        // Dates and timestamps outside the seekdb calendar range are invalid.
        if (micros < -62135596800000000LL || micros > 253402300799999999LL) { return false; }
        char buf[64];
        int64_t pos = 0;
        if (ObTimeConverter::datetime_to_str(micros, nullptr, col.type == Type::DATE ? 0 : 6,
                                            buf, sizeof(buf), pos) != OB_SUCCESS || pos < 10) {
          return false;
        }
        cell.text.assign(buf, col.type == Type::DATE ? 10 : pos);
      } else {
        cell.text = std::to_string(n);
      }
      break;
    }
    case 4:
    case 5: {
      double n = 0;
      if (col.physical == 4) {
        const uint32_t bits = values.little(4);
        float value = 0;
        memcpy(&value, &bits, sizeof(value));
        n = value;
      } else {
        const uint64_t bits = values.little(8);
        memcpy(&n, &bits, sizeof(n));
      }
      if (!std::isfinite(n)) { return false; }
      char buf[64];
      const auto result = std::to_chars(buf, buf + sizeof(buf), n);
      if (result.ec != std::errc{}) { return false; }
      cell.text.assign(buf, result.ptr);
      break;
    }
    case 6: {
      const uint64_t length = values.little(4);
      if (length > MAX_FIELD) { return false; }
      const auto data = values.take(length);
      if (!utf8(data)) { return false; }
      cell.text.assign(data);
      break;
    }
    default: return false;
  }
  return values.ok;
}

class ColumnReader
{
public:
  ColumnReader(Input &input, const Schema &column, const Chunk &chunk)
      : input_(input), schema_(column), chunk_(chunk),
        offset_(chunk.dictionary >= 0 ? chunk.dictionary : chunk.data),
        end_(offset_ + chunk.length) {}

  int next(Cell &cell)
  {
    int ret = OB_SUCCESS;
    while (rows_left_ == 0 && OB_SUCC(ret)) { ret = load_page(); }
    if (OB_FAIL(ret)) { return ret; }
    --rows_left_;
    uint32_t present = 1;
    if (schema_.repetition && (!levels_.next(present) || present > 1)) { return OB_INVALID_ARGUMENT; }
    if (!present) {
      cell = Cell{};
    } else if (encoding_ == 0) {
      if (!plain_cell(schema_, values_, plain_index_++, cell)) { ret = OB_INVALID_ARGUMENT; }
    } else {
      uint32_t index = 0;
      if (!indices_.next(index)) { ret = OB_INVALID_ARGUMENT; }
      else if (encoding_ == 3 && index <= 1) { cell = {Type::BOOL, index ? "true" : "false"}; }
      else if (index >= dictionary_.size()) { ret = OB_INVALID_ARGUMENT; }
      else { cell = dictionary_[index]; }
    }
    return ret;
  }

  size_t memory() const { return page_.capacity() + dictionary_bytes_; }

private:
  int load_page()
  {
    if (offset_ >= end_) { return OB_INVALID_ARGUMENT; }
    int ret = OB_SUCCESS;
    std::string head(std::min(end_ - offset_, MAX_PAGE_HEADER), '\0');
    if (OB_FAIL(input_.read_at(offset_, head.data(), head.size()))) { return ret; }
    Bytes reader{head};
    PageHeader h = page_header(reader);
    if (!reader.ok || h.compressed < 0 || h.uncompressed < 0 ||
        uint64_t(h.compressed) > MAX_PAGE || uint64_t(h.uncompressed) > MAX_PAGE ||
        reader.pos > end_ - offset_ || uint64_t(h.compressed) > end_ - offset_ - reader.pos) {
      return OB_INVALID_ARGUMENT;
    }
    offset_ += reader.pos;
    if (h.type == 1) { offset_ += h.compressed; return OB_SUCCESS; } // optional index page
    if (h.type != 0 && h.type != 2 && h.type != 3) { return OB_NOT_SUPPORTED; }
    if (h.rows < 0 || uint64_t(h.rows) > MAX_PAGE_VALUES || h.def_size < 0 || h.rep_size != 0) {
      return OB_INVALID_ARGUMENT;
    }
    if (h.type == 3 && (h.v2_rows != h.rows || h.nulls < 0 || h.nulls > h.rows)) {
      return OB_INVALID_ARGUMENT;
    }
    std::string packed(h.compressed, '\0');
    if (OB_FAIL(input_.read_at(offset_, packed.data(), packed.size()))) { return ret; }
    offset_ += h.compressed;
    const size_t prefix = h.type == 3 ? h.def_size : 0;
    if (prefix > packed.size() || prefix > uint64_t(h.uncompressed)) { return OB_INVALID_ARGUMENT; }
    if (chunk_.codec == 1 && (h.type != 3 || h.v2_compressed)) {
      std::string unpacked;
      if (!decompress_snappy(std::string_view(packed).substr(prefix), h.uncompressed - prefix, unpacked)) {
        return OB_INVALID_ARGUMENT;
      }
      page_.assign(packed, 0, prefix);
      page_.append(unpacked);
    } else {
      if (h.compressed != h.uncompressed) { return OB_INVALID_ARGUMENT; }
      page_ = std::move(packed);
    }
    Bytes data{page_};
    if (h.type == 2) {
      if (dictionary_seen_ || emitted_ > 0 || (h.encoding != 0 && h.encoding != 2)) { return OB_NOT_SUPPORTED; }
      dictionary_seen_ = true;
      dictionary_.reserve(h.rows);
      dictionary_bytes_ = h.rows * sizeof(Cell);
      for (int64_t i = 0; i < h.rows; ++i) {
        Cell cell;
        if (!plain_cell(schema_, data, i, cell)) { return OB_INVALID_ARGUMENT; }
        dictionary_bytes_ += cell.text.capacity();
        if (dictionary_bytes_ > MAX_ACTIVE_BYTES / 2) { return OB_SIZE_OVERFLOW; }
        dictionary_.push_back(std::move(cell));
      }
    } else {
      if (h.rows > chunk_.count - emitted_) { return OB_INVALID_ARGUMENT; }
      emitted_ += h.rows;
      if (schema_.repetition) {
        if (h.type == 0 && h.def_encoding != 3) { return OB_NOT_SUPPORTED; }
        uint64_t length = h.type == 0 ? data.little(4) : prefix;
        levels_.reset(data.take(length), 1);
      } else if (prefix != 0) {
        return OB_INVALID_ARGUMENT;
      }
      encoding_ = h.encoding;
      if (encoding_ == 2 || encoding_ == 8) {
        if (!dictionary_seen_) { return OB_INVALID_ARGUMENT; }
        if (data.left() > 0) {
          const unsigned width = data.byte();
          indices_.reset(data.take(data.left()), width);
        } else {
          indices_.reset({}, 0);
        }
      } else if (encoding_ == 3 && schema_.physical == 0) {
        const uint64_t length = data.little(4);
        indices_.reset(data.take(length), 1);
      } else if (encoding_ != 0) {
        return OB_NOT_SUPPORTED;
      }
      values_ = data;
      plain_index_ = 0;
      rows_left_ = h.rows;
    }
    return data.ok ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  }

  Input &input_;
  const Schema &schema_;
  const Chunk &chunk_;
  uint64_t offset_, end_;
  int64_t emitted_ = 0, rows_left_ = 0, encoding_ = -1;
  uint64_t plain_index_ = 0;
  bool dictionary_seen_ = false;
  std::string page_;
  Bytes values_{};
  Hybrid levels_, indices_;
  std::vector<Cell> dictionary_;
  size_t dictionary_bytes_ = 0;
};

int parquet_error(int ret)
{
  if (ret == OB_INVALID_ARGUMENT) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid or truncated Parquet metadata or page");
  } else if (ret == OB_NOT_SUPPORTED) {
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Parquet schema, compression, or encoding");
  }
  return ret;
}

} // namespace

struct Parquet::State
{
  explicit State(Input &source) : input(source) {}
  Input &input;
  Footer footer;
  std::vector<int> projection;
  std::vector<ColumnReader> readers;
  size_t group = 0;
  uint64_t row = 0;
};

Parquet::Parquet(Input &input) : state_(new (std::nothrow) State(input)) {}
Parquet::~Parquet() = default;

int Parquet::schema(Plan &plan)
{
  int ret = OB_SUCCESS;
  if (!state_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(read_footer(state_->input, state_->footer))) {
  } else {
    plan.row_count = state_->footer.rows;
    for (const auto &c : state_->footer.columns) {
      const auto name = ObString(c.name.size(), c.name.data());
      if (std::any_of(plan.columns.begin(), plan.columns.end(), [&](const Column &x) {
            return name.case_compare(ObString(x.name.size(), x.name.data())) == 0;
          })) {
        ret = OB_INVALID_ARGUMENT;
        break;
      }
      const int16_t scale = c.type == Type::DATETIME ? (c.time_unit == 1 ? 3 : 6) : 0;
      plan.columns.push_back({c.name, c.type, c.repetition == 1, scale});
    }
  }
  return parquet_error(ret);
}

int Parquet::open(const Plan &plan, const std::vector<int> &projection)
{
  int ret = OB_SUCCESS;
  if (!state_) { ret = OB_ALLOCATE_MEMORY_FAILED; }
  else if (OB_FAIL(read_footer(state_->input, state_->footer))) {}
  else if (state_->footer.rows != plan.row_count || state_->footer.columns.size() != plan.columns.size()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    state_->projection = projection;
  }
  return parquet_error(ret);
}

int Parquet::rescan()
{
  state_->readers.clear();
  state_->group = 0;
  state_->row = 0;
  return state_->input.check();
}

int Parquet::next(std::vector<Cell> &cells)
{
  int ret = OB_SUCCESS;
  auto &s = *state_;
  cells.clear();
  while (s.group < s.footer.groups.size() && s.row == uint64_t(s.footer.groups[s.group].rows)) {
    s.readers.clear();
    s.row = 0;
    ++s.group;
  }
  if (s.group == s.footer.groups.size()) { return OB_ITER_END; }
  if (s.row == 0) {
    const Group &group = s.footer.groups[s.group];
    s.readers.reserve(s.projection.size());
    for (int col : s.projection) {
      s.readers.emplace_back(s.input, s.footer.columns[col], group.columns[col]);
    }
  }
  size_t bytes = 0, memory = 0;
  for (auto &reader : s.readers) {
    Cell cell;
    if (OB_FAIL(reader.next(cell))) { break; }
    bytes += cell.text.size();
    memory += reader.memory();
    if (bytes > MAX_RECORD || memory > MAX_ACTIVE_BYTES) { ret = OB_SIZE_OVERFLOW; break; }
    cells.push_back(std::move(cell));
  }
  if (OB_SUCC(ret)) { ++s.row; }
  return parquet_error(ret);
}

} // namespace oceanbase::sql::file_sql
