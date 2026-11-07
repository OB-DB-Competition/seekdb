#!/usr/bin/env python3
"""Exercise file SQL on a local seekdb instance with secure_file_priv = '/'.

Usage: python3 test_file_sql.py --client /path/to/obclient --port 10000
The optional --parquet checks require PyArrow to generate independent fixtures.
Use --socket on a dedicated instance to also test secure_file_priv changes.
"""
import argparse
import base64
from pathlib import Path
import re
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, default=10000)
    parser.add_argument("--parquet", action="store_true")
    parser.add_argument("--socket")
    args = parser.parse_args()
    command = [args.client, "-h127.0.0.1", f"-P{args.port}", "-uroot", "-Dtest",
               "-N", "-B", "--raw", "--default-character-set=utf8mb4"]
    checks = 0

    def check(query, expected=None, error=None, enable=True, socket=False):
        nonlocal checks
        statement = ("set session enable_file_sql=on; " if enable else "") + query
        connection = ([args.client, "--protocol=socket", "-S", args.socket] + command[3:]
                      if socket else command)
        proc = subprocess.run(connection + ["-e", statement], capture_output=True,
                              text=True, timeout=30)
        if error is None:
            assert proc.returncode == 0, (query, proc.stderr)
        else:
            assert proc.returncode != 0 and re.search(rf"ERROR {error}\b", proc.stderr), (query, proc.stderr)
        if expected is not None:
            assert proc.stdout.rstrip("\n") == expected, (query, expected, proc.stdout)
        checks += 1
        return proc.stdout

    with tempfile.TemporaryDirectory(prefix="seekdb-file-sql-") as directory:
        root = Path(directory)
        csv = root / "records.csv"
        csv.write_bytes(('\ufeffid,name,name,day,created,empty\r\n'
                         '1,"中\n文","a""b",2024-02-29,2024-02-29T12:00:00,""\r\n'
                         '2,Tom,"",,2024-03-01,\r\n'
                         '3,Ada,c,2024-03-02,2024-03-02,null').encode())
        jsonl = root / "records.jsonl"
        jsonl.write_text('{"id":1,"v":10,"s":"\\ud83d\\ude42"}\n\n'
                         '{"id":2,"v":"20","later":true}\n', encoding="utf-8")
        bad_csv = root / "bad.csv"
        bad_csv.write_text('id,name\n1,"x"y\n', encoding="utf-8")
        bad_utf8 = root / "bad-utf8.csv"
        bad_utf8.write_bytes(b'id,name\n1,\xc0\x80\n')
        nested = root / "nested.jsonl"
        nested.write_text('{"id":1,"obj":{"a":2}}\n')
        big = root / "big.csv"
        big.write_text("id,value\n" + "".join(f"{i},v{i:05d}\n" for i in range(1, 5001)))
        numbers = root / "numbers.csv"
        numbers.write_text("lo,overflow,fp,bad_date,n\n"
                           "-9223372036854775808,9223372036854775808,1,1900-02-29,\n"
                           "9223372036854775807,99999999999999999999,2.5,1900-02-29,\n")
        export = root / "export.csv"
        invalid = root / "invalid.parquet"
        invalid.write_bytes(b"PAR1invalidPAR1")

        check("select @@session.enable_file_sql", "0", enable=False)
        check(f"select * from read_csv('{root}/nonexistent')", error=1235, enable=False)
        check(f"select * from read_csv('{csv}', 'xml')", error=1064)
        check(f"select * from read_csv('{csv}', sample_size=10)", error=1235)
        check(f"select * from read_csv('{csv}', header=true, header=false)", error=1210)
        check(f"select * from read_csv('{bad_csv}')", error=1210)
        check(f"select * from read_csv('{bad_utf8}')", error=1210)
        check(f"select * from read_csv('{root}')", error=1210)
        check(f"select * from read_csv('{root}/nonexistent')", error=1017)
        check(f"select * from read_json('{nested}')", error=1235)
        check(f"select * from read_parquet('{invalid}')", error=1210)
        check(f"select f.id, replace(f.name, char(10), '|'), f.name__2, f.empty is null "
              f"from READ_CSV('{csv}',sample_size=-1) AS f order by f.id",
              '1\t中|文\ta"b\t0\n2\tTom\t\t1\n3\tAda\tc\t0')
        check(f"select id, v, later from read_ndjson('{jsonl}') order by id",
              "1\t10\tNULL\n2\t20\t1")
        check(f"select c.id,j.v from read_csv('{csv}') c left join read_json('{jsonl}') j "
              "on j.id=c.id order by c.id", "1\t10\n2\t20\n3\tNULL")
        check(f"select id from read_csv('{csv}') c join read_json('{jsonl}') j on j.id=c.id",
              error=1052)
        check(f"select c.id,(select count(*) from read_json('{jsonl}') j where j.id<=c.id) "
              f"from read_csv('{csv}') c order by c.id", "1\t1\n2\t2\n3\t2")
        check(f"select count(*),sum(id),min(value),max(value) from read_csv('{big}')",
              "5000\t12502500\tv00001\tv05000")
        check(f"select id*2,upper(value) from read_csv('{big}') f where id<=20 "
              "order by id desc limit 3 offset 1", "38\tV00019\n36\tV00018\n34\tV00017")
        check(f"select mod(id,2),count(*),sum(id) from read_csv('{big}') where id<=20 "
              "group by mod(id,2) having sum(id)>90 order by 1", "0\t10\t110\n1\t10\t100")
        check(f"select count(*) from read_csv('{big}'); select count(*) from read_csv('{csv}')", "5000\n3")
        check(f"describe select * from read_csv('{numbers}')",
              "lo\tBIGINT\tNO\tNULL\tNULL\tNULL\n"
              "overflow\tVARCHAR\tNO\tNULL\tNULL\tNULL\n"
              "fp\tDOUBLE\tNO\tNULL\tNULL\tNULL\n"
              "bad_date\tVARCHAR\tNO\tNULL\tNULL\tNULL\n"
              "n\tVARCHAR\tYES\tNULL\tNULL\tNULL")
        assert "FILE SCAN" in check(f"explain select count(*) from read_csv('{big}') where id<5")
        assert len(check("desc select 1").splitlines()) > 2
        check(f"select substring_index(file,'/',-1) from glob('{root}/records.*') order by file",
              "records.csv\nrecords.jsonl")
        check(f"select * from read_csv('{csv}') for update", error=1235)

        check(f"copy (select 1 as id,null as a,'' as b,'NULL' as c) to '{export}' "
              "(format csv,header,nullstr 'NULL')")
        check(f"select id,a is null,b is null,c is null,c from read_csv('{export}',nullstr='NULL')",
              "1\t1\t0\t0\tNULL")
        check(f"copy (select 2) to '{export}'", error=1086)
        failed = root / "failed.csv"
        check(f"copy (select cast('{{\"a\":1}}' as json)) to '{failed}'", error=1235)
        assert not failed.exists() and not list(root.glob(".seekdb-copy-*"))
        union_export = root / "union.csv"
        check(f"copy (select id from read_csv('{csv}') union all select 9) "
              f"to '{union_export}' with (header false, delimiter '|')")
        assert sorted(union_export.read_text().splitlines()) == ["1", "2", "3", "9"]
        table_export = root / "table.csv"
        table = root.name.replace("-", "_")
        check(f"create table {table} (id bigint)", enable=False)
        try:
            check(f"insert into {table} values (41),(42); copy {table} to '{table_export}'")
            check(f"select sum(id) from read_csv('{table_export}')", "83")
            check(f"select sum(c.id) from read_csv('{big}') c join {table} t on c.id=t.id", "83")
        finally:
            check(f"drop table {table}", enable=False)

        if args.socket:
            allowed = root / "allowed"
            outside = root / "allowed-other"
            allowed.mkdir()
            outside.mkdir()
            inner_csv = allowed / "inner.csv"
            outer_csv = outside / "outer.csv"
            inner_csv.write_text("id\n1\n")
            outer_csv.write_text("id\n2\n")
            symlink = allowed / "link.csv"
            symlink.symlink_to(outer_csv)
            check(f"set global secure_file_priv='{allowed}'", enable=False, socket=True)
            try:
                check(f"select id from read_csv('{inner_csv}')", "1")
                check(f"select id from read_csv('{outer_csv}')", error=1227)
                check(f"select id from read_csv('{allowed}/../allowed-other/outer.csv')", error=1227)
                check(f"select id from read_csv('{symlink}')", error=1227)
                check(f"select file from glob('{allowed}/*.csv')", error=1227)
                check(f"copy (select 1) to '{outside}/denied.csv'", error=1227)
                assert not (outside / "denied.csv").exists()
            finally:
                check("set global secure_file_priv='/'", enable=False, socket=True)

        fixture = Path(__file__).resolve().parents[4] / (
            "tools/deploy/mysql_test/test_suite/file_sql/data/simple.parquet.b64")
        simple = root / "simple.parquet"
        simple.write_bytes(base64.b64decode(fixture.read_bytes()))
        check(f"select count(*),min(c1),max(c1),sum(c1) from read_parquet('{simple}')",
              "7\t0\t6\t21")

        if args.parquet:
            import datetime
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"id": [1, 2, 3, 4, 5, 6], "s": ["a", "b", None, "a", "b", "c"],
                              "paid": [True, False, None, True, False, True],
                              "day": [datetime.date(2024, 2, 29)] * 6})
            for version in ("1.0", "2.0"):
                parquet = root / f"groups-{version}.parquet"
                pq.write_table(table, parquet, compression="snappy",
                               row_group_size=2, data_page_version=version)
                check(f"select count(*),sum(id),count(s),sum(paid),min(day) from read_parquet('{parquet}')",
                      "6\t21\t5\t3\t2024-02-29")
                check(f"select c.id,p.s from read_csv('{csv}') c join read_parquet('{parquet}') p "
                      "on c.id=p.id order by c.id", "1\ta\n2\tb\n3\tNULL")

            damaged = root / "damaged-column.parquet"
            pq.write_table(table, damaged, compression="none", use_dictionary=False)
            metadata = pq.read_metadata(damaged)
            offset = metadata.row_group(0).column(1).data_page_offset
            with damaged.open("r+b") as stream:
                stream.seek(offset)
                stream.write(bytes(1))
            check(f"select count(*),sum(id) from read_parquet('{damaged}')", "6\t21")
            check(f"select s from read_parquet('{damaged}')", error=1210)

    print(f"PASS: {checks} file SQL checks")


if __name__ == "__main__":
    main()
