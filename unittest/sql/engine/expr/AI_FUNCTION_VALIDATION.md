# AI function enhancement validation

Branch base: `2026-competition` (`5bce9e06d`).

The six changes run in the SQL engine: typed/LOB/nested Prompt arguments,
completion config and content handling, native keyless Ollama endpoints,
validated embedding indices and vectors, globally sorted batched reranking,
and grouped multi-input vectorized embedding evaluation (20 inputs/request).

The existing official tests and expected outputs are unchanged.
`ai_prompt` defines the compatibility behavior: SQL NULL arguments fail with
5083, BLOB templates are accepted, and placeholder indices are checked when
rendering for completion rather than when constructing a Prompt object.

## Reproduce

Build with `./build.sh release --make -j4`, then replace both the deployment
and instance binaries and restart the instance as described in the lab manual.

Run the self-contained regression against a local test database:

```sh
python3 unittest/sql/engine/expr/ai_function_regression.py \
  --client deps/3rd/u01/obclient/bin/obclient --port 10000
```

It starts its own HTTP mock on an ephemeral local port, creates models prefixed
`reg_`, and cleans them up. Use a disposable test database. Assertions cover
request payloads/counts, 20-input boundaries, heterogeneous dimensions, shuffled
responses, NULL, filtering/LIMIT, long text, malformed JSON, incorrect vectors,
invalid config and reranking indices/scores. No real model is called.

For the unmodified official suite, start the same mock on port 8000:

```sh
python3 unittest/sql/engine/expr/ai_function_regression.py --serve
# In another terminal:
cd tools/deploy
./obd.sh mysqltest -n single --disable-reboot --suite ai_function
```

The local deterministic mock validates protocol behavior. It does not replace
the organizer's hidden service or establish a competition score.

## Results

- Release build succeeds.
- Self-contained SQL integration regression passes.
- Official `ai_function_enhancement`, `ai_prompt`, `ai_model_ddl`, and both
  `ai_model_lower_case_name` tests pass against the local mock.
- `ai_model_endpoint_ddl` fails on its first `max_concurrency` parameter:
  error 11115. The baseline endpoint struct/parser/storage have no such field;
  this is a pre-existing mismatch between the baseline and that test. This
  unrelated metadata feature is not silently ignored or simulated here.

The pre-existing local edit to `tools/deploy/activate_obd.sh` is excluded
from the submission commit.
