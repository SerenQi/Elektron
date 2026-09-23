import ast
import asyncio
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import types
import unittest
from unittest.mock import patch

from context_index import ensure_index, insert_raw
from recall_engine import RecallHistory, recall_candidates
import import_context
import liminal_ext


def memory_db():
    c = sqlite3.connect(':memory:')
    c.execute('CREATE VIRTUAL TABLE raw USING fts5(ts,source,role,text)')
    c.execute('CREATE TABLE import_state(file TEXT PRIMARY KEY,lines_done INT,bytes_done INT,mtime REAL)')
    ensure_index(c)
    return c


class IndexTests(unittest.TestCase):
    def test_backfill_atomic_and_idempotent(self):
        c = memory_db()
        c.execute('INSERT INTO raw VALUES(?,?,?,?)', ('2000-01-01','test','user','杯子'))
        ensure_index(c)
        ensure_index(c)
        self.assertEqual(c.execute('SELECT count(*) FROM raw_tri').fetchone()[0], 1)
        c.commit()
        insert_raw(c, '2000-01-02', 'test', 'thinking', 'another')
        c.rollback()
        self.assertEqual(c.execute('SELECT count(*) FROM raw').fetchone()[0], 1)
        self.assertEqual(c.execute('SELECT count(*) FROM raw_tri').fetchone()[0], 1)

    def test_half_line_both_importers_and_incremental_index(self):
        for transcript in (False, True):
            with self.subTest(transcript=transcript):
                c = memory_db()
                entry = ({'type':'user','timestamp':'2000-01-01','message':{'role':'user','content':'调料台'}}
                         if transcript else {'ts':'2000-01-01','role':'user','text':'杯子'})
                full = json.dumps(entry) + '\n'
                phase = {'text': full[:20], 'mtime': 1}
                fake_os = types.SimpleNamespace(path=types.SimpleNamespace(getmtime=lambda p:phase['mtime'], basename=lambda p:p))
                with patch.object(import_context, 'os', fake_os), patch.object(import_context, 'open', lambda *a,**k:io.StringIO(phase['text']), create=True):
                    run = lambda: import_context.import_transcript(c, 'fake.jsonl') if transcript else import_context.import_chat(c)
                    run()
                    phase.update(text=full, mtime=2)
                    run()
                    run()
                self.assertEqual(c.execute('SELECT count(*) FROM raw').fetchone()[0], 1)
                self.assertEqual(c.execute('SELECT count(*) FROM raw_tri').fetchone()[0], 1)


class FakeEngine:
    enabled = True
    async def search_similar(self, *a, **kw):
        return [('semantic', .85)]


class Manager:
    embedding_engine = FakeEngine()
    async def list_all(self, **kw):
        return [
            {'id':'lexical','metadata':{'name':'cup'},'content':'cup'},
            {'id':'semantic','metadata':{'name':'old event'},'content':'unrelated wording'},
            {'id':'resolved','metadata':{'resolved':True},'content':'cup'},
        ]


class RecallTests(unittest.IsolatedAsyncioTestCase):
    async def test_vector_and_keyword_union_and_resolved(self):
        rows = await recall_candidates(Manager(), 'cup', threshold=75)
        self.assertEqual({b['id'] for b in rows}, {'lexical','semantic','resolved'})
        self.assertEqual(rows[-1]['id'], 'resolved')

    async def test_slow_vector_retains_keywords(self):
        class Slow(FakeEngine):
            async def search_similar(self,*a,**kw):
                await asyncio.sleep(1)
                return []
        m=Manager();m.embedding_engine=Slow()
        rows=await recall_candidates(m,'cup',vector_timeout=.01)
        self.assertIn('lexical', {b['id'] for b in rows})

    async def test_session_isolation_expiry_and_tool_schema(self):
        h=RecallHistory(ttl=10)
        with patch('recall_engine.time.monotonic',return_value=1):
            h.mark('a','x')
            self.assertTrue(h.seen('a','x'))
            self.assertFalse(h.seen('b','x'))
        with patch('recall_engine.time.monotonic',return_value=12):
            self.assertFalse(h.seen('a','x'))
        from mcp.server.fastmcp import Context, FastMCP
        root = Path(__file__).parent
        if not (root / 'server.py').exists():
            root = root.parent
        tree=ast.parse((root / 'server.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='recall')
        fn.decorator_list=[]
        ns={'Context':Context,'config':{},'bucket_mgr':Manager(),'recall_candidates':recall_candidates,
            '_recall_history':RecallHistory(),'strip_wikilinks':lambda x:x,'_track_label':lambda x:''}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'recall-test','exec'),ns)
        mcp=FastMCP('audit')
        mcp.tool()(ns['recall'])
        tools=await mcp.list_tools()
        self.assertNotIn('ctx',tools[0].inputSchema['properties'])
        a=types.SimpleNamespace(session=object());b=types.SimpleNamespace(session=object())
        self.assertTrue(await ns['recall']('cup',a))
        self.assertFalse(await ns['recall']('cup',a))
        self.assertTrue(await ns['recall']('cup',b))
        self.assertTrue(await ns['recall']('cup',a,refresh=True))

    async def test_live_entry_short_words_and_semantic_callback(self):
        class MCP:
            def __init__(self):self.tools={};self.routes={}
            def tool(self,name):return lambda fn:self.tools.setdefault(name,fn)
            def custom_route(self,path,methods):return lambda fn:self.routes.setdefault(path,fn)
        with tempfile.TemporaryDirectory() as folder:
            c=sqlite3.connect(str(Path(folder)/'context.db'))
            c.execute('CREATE VIRTUAL TABLE raw USING fts5(ts,source,role,text)')
            ensure_index(c)
            insert_raw(c,'2000-01-01','test','user','我拿起杯子喝水')
            insert_raw(c,'2000-01-02','test','thinking','调料台应该在门口')
            c.commit();c.close()
            mcp=MCP();liminal_ext.register(mcp,folder,bucket_manager=Manager())
            short=await mcp.tools['magnet_recall']('杯子')
            long=await mcp.tools['magnet_recall']('调料台')
            self.assertIn('我拿起杯子喝水',short)
            self.assertIn('我(想)',long)
            self.assertIn('unrelated wording',short)
            req=types.SimpleNamespace(query_params={'q':'杯子'})
            response=await mcp.routes['/liminal/magnet'](req)
            self.assertIn('我拿起杯子喝水',json.loads(response.body)['block'])


if __name__ == '__main__':
    unittest.main()
