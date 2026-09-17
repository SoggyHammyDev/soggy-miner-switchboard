#!/usr/bin/env python3
import asyncio, json, os, time
from pathlib import Path

DATA_DIR = Path(os.environ.get('DATA_DIR','/data'))
CONFIG_PATH = DATA_DIR/'config.json'
DEFAULT = {
  'listen_host':'0.0.0.0', 'listen_port':3338,
  'pools':{},
  'routes':{},
}

def load_config():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT, indent=2))
        return json.loads(json.dumps(DEFAULT))
    try:
        value=json.loads(CONFIG_PATH.read_text())
        if not isinstance(value,dict): raise ValueError
        return {**DEFAULT, **value}
    except Exception:
        return json.loads(json.dumps(DEFAULT))

def save_config(cfg):
    tmp=CONFIG_PATH.with_suffix('.tmp')
    tmp.write_text(json.dumps(cfg, indent=2))
    os.replace(tmp, CONFIG_PATH)

class Switchboard:
    def __init__(self):
        self.config=load_config()
        self.lock=asyncio.Lock()
        self.sessions={}
        self.session_seq=0

    async def snapshot(self):
        async with self.lock:
            return {
                'config': self.config,
                'sessions': [dict(v) for v in self.sessions.values()],
                'updated': int(time.time())
            }

    async def set_route(self, ip, pool):
        async with self.lock:
            if pool not in self.config.get('pools',{}):
                raise ValueError('Unknown pool')
            self.config.setdefault('routes',{})[ip]=pool
            save_config(self.config)
            victims=[sid for sid,s in self.sessions.items() if s['ip']==ip]
            for sid in victims:
                self.sessions[sid]['requested_disconnect']=True
                writer=self.sessions[sid].get('_down_writer')
                if writer:
                    writer.close()
            return victims

    async def add_pool(self, name, host, port):
        if not name or not host or not (1 <= int(port) <= 65535):
            raise ValueError('Invalid pool')
        async with self.lock:
            self.config.setdefault('pools',{})[name]={'host':host,'port':int(port)}
            save_config(self.config)

    async def handle_miner(self, reader, writer):
        peer=writer.get_extra_info('peername') or ('unknown',0)
        ip=str(peer[0])
        async with self.lock:
            pool_name=self.config.get('routes',{}).get(ip)
            pool=self.config.get('pools',{}).get(pool_name) if pool_name else None
            self.session_seq += 1
            sid=self.session_seq
            self.sessions[sid]={'id':sid,'ip':ip,'pool':pool_name,'connected':int(time.time()),'bytes_in':0,'bytes_out':0,'worker':'','status':'connecting','_down_writer':writer}
        if not pool:
            async with self.lock: self.sessions[sid]['status']='no route configured'
            writer.close(); await writer.wait_closed()
            async with self.lock: self.sessions.pop(sid,None)
            return
        try:
            up_reader, up_writer = await asyncio.open_connection(pool['host'], int(pool['port']))
            async with self.lock: self.sessions[sid]['status']='connected'
            async def downstream_to_upstream():
                while True:
                    line=await reader.readline()
                    if not line: break
                    try:
                        msg=json.loads(line)
                        if msg.get('method')=='mining.authorize' and isinstance(msg.get('params'),list) and msg['params']:
                            async with self.lock: self.sessions[sid]['worker']=str(msg['params'][0])[:160]
                    except Exception: pass
                    up_writer.write(line); await up_writer.drain()
                    async with self.lock: self.sessions[sid]['bytes_in'] += len(line)
            async def upstream_to_downstream():
                while True:
                    data=await up_reader.read(65536)
                    if not data: break
                    writer.write(data); await writer.drain()
                    async with self.lock: self.sessions[sid]['bytes_out'] += len(data)
            t1=asyncio.create_task(downstream_to_upstream())
            t2=asyncio.create_task(upstream_to_downstream())
            done,pending=await asyncio.wait({t1,t2}, return_when=asyncio.FIRST_COMPLETED)
            for t in pending: t.cancel()
            up_writer.close(); await up_writer.wait_closed()
        except Exception as e:
            async with self.lock:
                if sid in self.sessions: self.sessions[sid]['status']='error: '+str(e)[:120]
        finally:
            writer.close()
            try: await writer.wait_closed()
            except Exception: pass
            async with self.lock: self.sessions.pop(sid,None)

async def run_proxy(board):
    cfg=board.config
    server=await asyncio.start_server(board.handle_miner,cfg['listen_host'],int(cfg['listen_port']))
    async with server: await server.serve_forever()
