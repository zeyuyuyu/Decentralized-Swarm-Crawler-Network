import asyncio
import aiohttp
from typing import Dict, List, Optional
from dataclasses import dataclass
import time

@dataclass
class CrawlTask:
    url: str
    depth: int
    timestamp: float
    retries: int = 0

class CrawlerNode:
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers
        self.work_queue: Dict[str, CrawlTask] = {}
        self.results: Dict[str, dict] = {}
        self.last_heartbeat: Dict[str, float] = {}
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        asyncio.create_task(self._heartbeat_monitor())
        asyncio.create_task(self._work_redistributor())

    async def stop(self):
        if self.session:
            await self.session.close()

    async def add_task(self, url: str, depth: int) -> str:
        task_id = f"{url}:{int(time.time())}"
        self.work_queue[task_id] = CrawlTask(
            url=url,
            depth=depth,
            timestamp=time.time()
        )
        return task_id

    async def _crawl_url(self, task: CrawlTask) -> dict:
        if not self.session:
            raise RuntimeError("Session not initialized")

        try:
            async with self.session.get(task.url) as response:
                if response.status == 200:
                    text = await response.text()
                    return {
                        "url": task.url,
                        "status": response.status,
                        "content": text,
                        "timestamp": time.time()
                    }
                return {
                    "url": task.url,
                    "status": response.status,
                    "error": "Non-200 status code"
                }
        except Exception as e:
            return {
                "url": task.url,
                "status": -1,
                "error": str(e)
            }

    async def _heartbeat_monitor(self):
        while True:
            current_time = time.time()
            for peer in self.peers:
                try:
                    if current_time - self.last_heartbeat.get(peer, 0) > 30:
                        async with aiohttp.ClientSession() as session:
                            async with session.get(f"{peer}/health") as response:
                                if response.status == 200:
                                    self.last_heartbeat[peer] = current_time
                except:
                    print(f"Peer {peer} appears to be down")
            await asyncio.sleep(10)

    async def _work_redistributor(self):
        while True:
            current_time = time.time()
            # Find stale tasks
            stale_tasks = [
                task_id for task_id, task in self.work_queue.items()
                if current_time - task.timestamp > 300 and task.retries < 3
            ]

            # Redistribute to healthy peers
            healthy_peers = [
                peer for peer in self.peers
                if current_time - self.last_heartbeat.get(peer, 0) < 30
            ]

            if healthy_peers and stale_tasks:
                for task_id in stale_tasks:
                    task = self.work_queue[task_id]
                    task.retries += 1
                    task.timestamp = current_time
                    # Round-robin distribution
                    target_peer = healthy_peers[task.retries % len(healthy_peers)]
                    try:
                        async with aiohttp.ClientSession() as session:
                            await session.post(
                                f"{target_peer}/task\