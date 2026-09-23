#!/usr/bin/env python3
import asyncio, json, os, time
from pathlib import Path

DATA_DIR = Path(os.environ.get('DATA_DIR','/data'))
CONFIG_PATH = DATA_DIR/'config.json'
DEFAULT = {
  'listen_host':'0.0.0.0', 'listen_port':3338,
  'pools':{},
  'routes':{},
  'groups':{},
}

def load_config():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT, indent=2))
        return json.loads(json.dumps(DEFAULT))
    try:
        value=json.loads(CONFIG_PATH.read_text())
        if not isinstance(value,dict): raise ValueError
        merged={**DEFAULT, **value}
        merged.setdefault('groups',{})
        return merged
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

    def _match_group(self, worker):
        worker=str(worker or '')
        best=None
        for name,g in self.config.get('groups',{}).items():
            prefix=str(g.get('prefix',''))
            if prefix and worker.startswith(prefix):
                candidate=(len(prefix),name,g)
                if best is None or candidate[0] > best[0]:
                    best=candidate
        return (best[1],best[2]) if best else (None,None)

    async def snapshot(self):
        async with self.lock:
            return {
                'config': self.config,
                'sessions': [{k:v for k,v in s.items() if not k.startswith('_')} for s in self.sessions.values()],
                'updated': int(time.time())
            }

    async def _disconnect(self, predicate):
        victims=[]
        for sid,s in self.sessions.items():
            if predicate(s):
                victims.append(sid)
                s['requested_disconnect']=True
                writer=s.get('_down_writer')
                if writer: writer.close()
        return victims

    async def set_route(self, ip, pool):
        async with self.lock:
            if pool not in self.config.get('pools',{}): raise ValueError('Unknown pool')
            self.config.setdefault('routes',{})[ip]=pool
            save_config(self.config)
            return await self._disconnect(lambda s:s['ip']==ip)

    async def set_group(self, name, prefix, pool):
        name=str(name or '').strip(); prefix=str(prefix or '').strip()
        if not name or not prefix: raise ValueError('Group name and worker prefix are required')
        async with self.lock:
            if pool not in self.config.get('pools',{}): raise ValueError('Unknown pool')
            self.config.setdefault('groups',{})[name]={'prefix':prefix,'pool':pool}
            save_config(self.config)
            return await self._disconnect(lambda s:str(s.get('worker','')).startswith(prefix))

    async def delete_group(self, name):
        async with self.lock:
            group=self.config.setdefault('groups',{}).pop(name,None)
            if not group: raise ValueError('Unknown group')
            save_config(self.config)
            prefix=str(group.get('prefix',''))
            return await self._disconnect(lambda s:prefix and str(s.get('worker','')).startswith(prefix))

    async def add_pool(self, name, host, port):
        if not name or not host or not (1 <= int(port) <= 65535): raise ValueError('Invalid pool')
        async with self.lock:
            self.config.setdefault('pools',{})[name]={'host':host,'port':int(port)}
            save_config(self.config)

    async def handle_miner(self, reader, writer):
        peer=writer.get_extra_info('peername') or ('unknown',0); ip=str(peer[0])
        async with self.lock:
            self.session_seq += 1; sid=self.session_seq
            self.sessions[sid]={'id':sid,'ip':ip,'pool':None,'group':None,'connected':int(time.time()),'bytes_in':0,'bytes_out':0,'worker':'','status':'authorizing','_down_writer':writer}

        # Buffer the initial Stratum messages until authorize tells us the worker.
        buffered=[]; worker=''
        try:
            while len(buffered) < 32:
                line=await asyncio.wait_for(reader.readline(), timeout=15)
                if not line: break
                buffered.append(line)
                try:
                    msg=json.loads(line)
                    if msg.get('method')=='mining.authorize' and isinstance(msg.get('params'),list) and msg['params']:
                        worker=str(msg['params'][0])[:160]; break
                except Exception: pass

            async with self.lock:
                group_name,group=self._match_group(worker)
                pool_name=group.get('pool') if group else self.config.get('routes',{}).get(ip)
                pool=self.config.get('pools',{}).get(pool_name) if pool_name else None
                self.sessions[sid].update({'worker':worker,'group':group_name,'pool':pool_name})

            if not pool:
                async with self.lock: self.sessions[sid]['status']='no route configured'
                return

            up_reader,up_writer=await asyncio.open_connection(pool['host'],int(pool['port']))
            for line in buffered:
                up_writer.write(line)
            await up_writer.drain()
            async with self.lock: self.sessions[sid]['status']='connected'

            async def downstream_to_upstream():
                while True:
                    line=await reader.readline()
                    if not line: break
                    up_writer.write(line); await up_writer.drain()
                    async with self.lock: self.sessions[sid]['bytes_in'] += len(line)
            async def upstream_to_downstream():
                while True:
                    data=await up_reader.read(65536)
                    if not data: break
                    writer.write(data); await writer.drain()
                    async with self.lock: self.sessions[sid]['bytes_out'] += len(data)
            t1=asyncio.create_task(downstream_to_upstream()); t2=asyncio.create_task(upstream_to_downstream())
            done,pending=await asyncio.wait({t1,t2},return_when=asyncio.FIRST_COMPLETED)
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
