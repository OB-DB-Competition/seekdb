#!/usr/bin/env python3
"""SQL integration regression for AI functions using a local deterministic mock.

Run against a disposable local test database:
  python3 unittest/sql/engine/expr/ai_function_regression.py --client PATH --port 10000
Use --serve to provide the mock on port 8000 for the official mysqltest suite.
No model service, Python packages, or external network access are required.
"""
import argparse
import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EVENTS = []


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        EVENTS.append((self.path, req))
        status = 200
        if self.path.endswith('/http500') or (self.path.endswith('/retry') and sum(path == self.path for path, _ in EVENTS) == 1):
            status, result = 500, {'error': 'temporary failure'}
        elif self.path.endswith('/malformed'):
            result = b'{broken'
        elif self.path.endswith('/root-array'):
            result = []
        elif '/chat/completions' in self.path:
            content = 'echo:' + req['messages'][-1]['content']
            result = {'choices': [{'message': {'content': content}}]}
            if self.path.endswith('/empty'):
                result['choices'][0]['message']['content'] = ''
            elif self.path.endswith('/bad-type'):
                result['choices'] = 4
            elif self.path.endswith('/structured'):
                result['choices'][0]['message']['content'] = {'answer': [1, True]}
        elif '/embeddings' in self.path or '/api/embed' in self.path:
            texts = req['input']
            assert isinstance(texts, list) and all(isinstance(t, str) for t in texts)
            dim = req.get('dimensions', 768 if '/embeddings' in self.path else 3)
            vectors = [[(len(t.encode()) + i) / 10 for i in range(dim)] for t in texts]
            if self.path.endswith('/second-bad') and sum(path == self.path for path, _ in EVENTS) == 2:
                vectors[0][0] = None
            if self.path.endswith('/bad-element'):
                vectors[0][0] = 'bad'
            elif self.path.endswith('/bad-dimension'):
                vectors[0].append(0.1)
            if '/api/embed' in self.path:
                result = {'embeddings': vectors}
            else:
                data = [{'index': i, 'embedding': v} for i, v in enumerate(vectors)]
                if self.path.endswith('/duplicate'):
                    data[-1]['index'] = data[0]['index']
                elif self.path.endswith('/out-of-range'):
                    data[0]['index'] = len(data)
                elif self.path.endswith('/missing'):
                    data.pop()
                result = {'data': list(reversed(data))}
        elif '/rerank' in self.path:
            results = [{'index': i, 'relevance_score': len(t) + 100 * t.count(req['query'])}
                       for i, t in enumerate(req['documents'])]
            if self.path.endswith('/duplicate'):
                results[-1]['index'] = results[0]['index']
            elif self.path.endswith('/bad-score'):
                results[0]['relevance_score'] = None
            elif self.path.endswith('/out-of-range'):
                results[0]['index'] = len(results)
            elif self.path.endswith('/missing'):
                results.pop()
            result = {'results': list(reversed(results))}
        elif '/api/generate' in self.path:
            assert req['stream'] is False
            result = {'done': True, 'response': 'echo:' + req['prompt']}
        else:
            status, result = 404, {'error': 'unknown protocol'}
        body = result if isinstance(result, bytes) else json.dumps(result).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client')
    parser.add_argument('--port', type=int, default=10000)
    parser.add_argument('--serve', action='store_true')
    args = parser.parse_args()
    server = ThreadingHTTPServer(('127.0.0.1', 8000 if args.serve else 0), Handler)
    if args.serve:
        server.serve_forever()
        return
    if not args.client:
        parser.error('--client is required unless --serve is used')
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = 'http://127.0.0.1:%d' % server.server_port
    checks = 0

    def sql(statement, error=None):
        nonlocal checks
        p = subprocess.run([args.client, '-h127.0.0.1', '-P%d' % args.port, '-uroot',
                            '-Dtest', '-N', '-B', '--raw', '--default-character-set=utf8mb4',
                            '-e', statement], capture_output=True, text=True)
        checks += 1
        if error:
            assert p.returncode and 'ERROR %d' % error in p.stderr, (statement, p.stdout, p.stderr)
        else:
            assert p.returncode == 0, (statement, p.stderr)
        return p.stdout.strip()

    def endpoint(model, path):
        sql("call DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT('reg_%s_ep', '%s')" %
            (model, json.dumps({'url': url + path})))

    try:
        for name, kind, provider, path in [('c', 'completion', 'openai', '/v1/chat/completions'),
                ('e', 'dense_embedding', 'openai', '/v1/embeddings'),
                ('r', 'rerank', 'siliconflow', '/v1/rerank'),
                ('oc', 'completion', 'Ollama', '/api/generate'),
                ('oe', 'dense_embedding', 'OLLAMA', '/api/embed')]:
            sql("call DBMS_AI_SERVICE.CREATE_AI_MODEL('reg_%s', '%s')" %
                (name, json.dumps({'type': kind, 'model_name': 'regression-model'})))
            cfg = {'ai_model_name': 'reg_' + name, 'url': url + path, 'provider': provider}
            if provider == 'openai' or provider == 'siliconflow':
                cfg['access_key'] = 'test-key'
            sql("call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('reg_%s_ep', '%s')" % (name, json.dumps(cfg)))
        assert sql("select ai_complete('reg_c', ai_prompt('{0}/{0}/{1}', 'long argument', json_array(1,true)))") == 'echo:long argument/long argument/[1, true]'
        assert sql("select ai_complete('reg_c',ai_prompt('outer {0}',ai_prompt('inner {0}','x')))") == 'echo:outer inner x'
        assert sql("select ai_complete('reg_c',ai_prompt('{{{0}}} {abc} {}','中文🙂'))") == 'echo:{中文🙂} {abc} {}'
        assert sql("select ai_complete('reg_c','hello',null)") == 'echo:hello'
        sql("select ai_complete('reg_c','x',json_object('temperature',0.2,'max_tokens',8))")
        assert EVENTS[-1][1]['temperature'] == 0.2 and EVENTS[-1][1]['max_tokens'] == 8
        for cfg in ["json_array(1)", "json_object('temperature','hot')", "json_object('max_tokens',0)", "json_object('model','override')"]:
            before = len(EVENTS)
            sql("select ai_complete('reg_c','x',%s)" % cfg, 1210)
            assert len(EVENTS) == before
        for index in ['5', '999999999999999999999999']:
            sql("select ai_prompt('{%s}','x')" % index)
            before = len(EVENTS)
            sql("select ai_complete('reg_c',ai_prompt('{%s}','x'))" % index, 1210)
            assert len(EVENTS) == before
        sql('create table reg_ai_rows(id int primary key, c longtext, d int)')
        sql("insert into reg_ai_rows values " + ','.join("(%d,%s,%d)" %
            (i, 'null' if i % 7 == 0 else "'%s'" % ('x' * i), 2 + i % 2) for i in range(1,46)))
        start = len(EVENTS)
        rows = sql("select id,ai_embed('reg_e',c,d) from reg_ai_rows order by id").splitlines()
        for i, row in enumerate(rows, 1):
            key, value = row.split('\t')
            assert int(key) == i
            assert value == 'NULL' if i % 7 == 0 else json.loads(value) == [(i+j)/10 for j in range(2+i%2)]
        requests = EVENTS[start:]
        assert all(len(r[1]['input']) <= 20 for r in requests)
        assert sum(len(r[1]['input']) for r in requests) == 39
        assert len(requests) == 2, 'expected actual multi-input HTTP batches: %r' % requests
        # One dimension across 39 non-NULL rows must split as 20 + 19.
        start = len(EVENTS)
        sql("select id,ai_embed('reg_e',c,3) from reg_ai_rows order by id")
        assert [len(r[1]['input']) for r in EVENTS[start:]] == [20, 19]
        # A failure in the second HTTP batch must fail the SQL expression.
        endpoint('e','/v1/embeddings/second-bad')
        sql("select id,ai_embed('reg_e',c,3) from reg_ai_rows order by id",4070)
        endpoint('e','/v1/embeddings')
        # Per-row model values form separate request groups.
        start = len(EVENTS)
        sql("select ai_embed(if(id%2=0,'reg_e','reg_oe'),c,3) from reg_ai_rows where id<6")
        assert sorted(len(r[1]['input']) for r in EVENTS[start:]) == [2,3]
        start = len(EVENTS)
        sql("select id,ai_embed('reg_e',c,3) from reg_ai_rows where id <= 10 order by id limit 3")
        assert sum(len(r[1]['input']) for r in EVENTS[start:]) == 3
        assert sql("select ai_embed('reg_e',null)") == 'NULL'
        for dim in ['0','-1','2.5','null']:
            sql("select ai_embed('reg_e','x',%s)" % dim, 1210)
        sql("update reg_ai_rows set c=repeat('长',100000) where id=1")
        assert sql("select json_unquote(json_extract(ai_prompt('{0}',c),'$.args[0]'))=c from reg_ai_rows where id=1") == '1'
        for suffix in ['bad-element','bad-dimension','out-of-range','missing','root-array']:
            endpoint('e', '/v1/embeddings/' + suffix)
            sql("select ai_embed('reg_e','x',3)", 4070)
        endpoint('e','/v1/embeddings/duplicate')
        sql("select ai_embed('reg_e',c,3) from reg_ai_rows where id between 2 and 4",4070)
        docs = "json_array(" + ','.join("'%s'" % ('a' * i) for i in range(1,46)) + ')'
        result = json.loads(sql("select ai_rerank('reg_r','q',%s)" % docs))
        assert [x['index'] for x in result] == list(reversed(range(45)))
        result = json.loads(sql("select ai_rerank('reg_r','q',json_array('aa','bb','cc'),null)"))
        assert [x['index'] for x in result] == [0,1,2]
        objects = "json_array(" + ','.join("json_object('id',%d,'text','%s','tag','keep')" % (i,'a'*i) for i in range(1,25)) + ')'
        result = json.loads(sql("select ai_rerank('reg_r','q',%s,'text')" % objects))
        assert [x['id'] for x in result] == list(reversed(range(1,25))) and all(x['tag']=='keep' for x in result)
        for suffix in ['duplicate','out-of-range','missing','bad-score']:
            endpoint('r','/v1/rerank/'+suffix)
            sql("select ai_rerank('reg_r','q',json_array('a','b'))",4070)
        for suffix in ['empty','bad-type','root-array']:
            endpoint('c','/v1/chat/completions/'+suffix)
            sql("select ai_complete('reg_c','x')",4070)
        endpoint('c','/v1/chat/completions/malformed')
        sql("select ai_complete('reg_c','x')",3140)
        endpoint('c','/v1/chat/completions/retry')
        start = len(EVENTS)
        assert sql("select ai_complete('reg_c','retry body')") == 'echo:retry body'
        assert len(EVENTS) - start == 2
        endpoint('c','/v1/chat/completions/structured')
        assert json.loads(sql("select ai_complete('reg_c','x')")) == {'answer':[1,True]}
        assert sql("select ai_complete('reg_oc','hello',json_object('temperature',0.1,'max_tokens',16))") == 'echo:hello'
        assert EVENTS[-1][1]['stream'] is False and EVENTS[-1][1]['options']['num_predict'] == 16
        assert len(json.loads(sql("select ai_embed('reg_oe','hello')"))) == 3
        print('PASS: %d SQL checks; batching, ordering, LOB, config, Ollama and invalid responses verified' % checks)
    finally:
        sql('drop table if exists reg_ai_rows')
        for name in ['c','e','r','oc','oe']:
            sql("call DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('reg_%s_ep')" % name)
            sql("call DBMS_AI_SERVICE.DROP_AI_MODEL('reg_%s')" % name)
        server.shutdown()


if __name__ == '__main__':
    main()
