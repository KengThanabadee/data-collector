"""Shared conservative public REST and connection reservations, never raw data."""
import asyncio
import fcntl
import json
import time

from .contracts import canonical
from .journal import _publish_bytes

RULES={'spot':(3000,60),'futures':(1200,60),'funding':(250,300)}


class Budget:
    def __init__(self,path,boot):
        self.path,self.boot=path,boot
        if not path.exists():_publish_bytes(path,canonical(dict(boot=boot,events={},remote={},next_http=0,next_connect=0)))

    def _edit(self,operation):
        with self.path.open('r+b',buffering=0) as file:
            try:fcntl.flock(file.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:return False
            try:
                body=file.read(65537)
                if len(body)>65536:raise ValueError('quota state byte bound')
                state=json.loads(body)
                if state['boot']!=self.boot:raise ValueError('quota process clock epoch differs')
                result=operation(state,time.monotonic_ns())
                if result:
                    body=canonical(state)
                    if len(body)>65536:raise ValueError('quota state byte bound')
                    file.seek(0);file.write(body);file.truncate()
                return result
            finally:fcntl.flock(file.fileno(),fcntl.LOCK_UN)

    def reserve(self,family,purpose):
        def edit(state,now):
            if now<state['next_http']:return False
            pool='spot' if family=='spot' else 'funding' if purpose=='funding' else 'futures'
            cost=250 if purpose=='depth' and family=='spot' else 20 if purpose=='depth' else 20 if family=='spot' else 1
            limit,seconds=RULES[pool];window=seconds*1_000_000_000
            events=[v for v in state['events'].get(pool,[]) if now-v[0]<window]
            remote=state['remote'].get(pool,[0,0])
            observed=remote[1] if now-remote[0]<window else 0
            if max(sum(v[1] for v in events),observed)+cost>limit:return False
            events.append([now,cost]);state['events'][pool]=events
            state['next_http']=now+1_000_000_000
            return True
        return self._edit(edit)

    def connect(self):
        def edit(state,now):
            if now<state['next_connect']:return False
            state['next_connect']=now+5_000_000_000
            return True
        return self._edit(edit)

    async def acquire(self,family,purpose,stopped):
        while not stopped():
            if self.reserve(family,purpose):return True
            await asyncio.sleep(.1)
        return False

    async def connection(self,stopped):
        while not stopped():
            if self.connect():return True
            await asyncio.sleep(.1)
        return False

    async def observe(self,family,headers,stopped):
        values={k.lower():v for k,v in headers}
        value=values.get('x-mbx-used-weight-1m',values.get('x-mbx-used-weight'))
        if value is None:return
        used=int(value)
        if not 0<=used<=10**9:raise ValueError('venue weight header range')
        pool='spot' if family=='spot' else 'futures'
        def edit(state,now):
            previous=state['remote'].get(pool,[0,0])
            if now-previous[0]<60_000_000_000:used_now=max(used,previous[1])
            else:used_now=used
            state['remote'][pool]=[now,used_now]
            return True
        while not stopped() and not self._edit(edit):await asyncio.sleep(.05)
