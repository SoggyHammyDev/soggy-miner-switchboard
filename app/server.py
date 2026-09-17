#!/usr/bin/env python3
import asyncio, json, mimetypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from switchboard import Switchboard, run_proxy

ROOT=Path(__file__).parent
STATIC=ROOT/'static'
board=Switchboard()
loop=asyncio.new_event_loop()

def call(coro):
    return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=5)

class Handler(BaseHTTPRequestHandler):
    def send_json(self,obj,code=200):
        data=json.dumps(obj).encode(); self.send_response(code); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_GET(self):
        if self.path=='/api/state': return self.send_json(call(board.snapshot()))
        path='index.html' if self.path=='/' else self.path.lstrip('/')
        f=(STATIC/path).resolve()
        if STATIC.resolve() not in f.parents and f!=STATIC.resolve(): self.send_error(404); return
        if not f.exists(): self.send_error(404); return
        data=f.read_bytes(); self.send_response(200); self.send_header('Content-Type',mimetypes.guess_type(f.name)[0] or 'application/octet-stream'); self.send_header('Content-Length',str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_POST(self):
        try:
            n=int(self.headers.get('Content-Length','0')); body=json.loads(self.rfile.read(n) or b'{}')
            if self.path=='/api/pools':
                call(board.add_pool(body['name'],body['host'],body['port'])); return self.send_json({'ok':True})
            if self.path=='/api/route':
                call(board.set_route(body['ip'],body['pool'])); return self.send_json({'ok':True,'message':'Route updated; miner connection recycled.'})
            self.send_error(404)
        except Exception as e: self.send_json({'error':str(e)},400)
    def log_message(self,*args): pass

def main():
    Thread(target=lambda: (asyncio.set_event_loop(loop), loop.run_until_complete(run_proxy(board))), daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0',8080),Handler).serve_forever()
if __name__=='__main__': main()
