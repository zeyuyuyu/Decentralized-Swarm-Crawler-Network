import asyncio
import aiohttp
from typing import Set, Dict, List
import json
import hashlib
from dataclasses import dataclass

@dataclass
class CrawlTask:
    url: str
    depth: int
    task_id: str = ''

    def __post_init__(self):
        if not self.task_id:
            self.task_id = hashlib.sha256(self.url.encode()).hexdigest()[:12]

class CrawlerNode:
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = set(peers)
        self.work_queue: asyncio.Queue[CrawlTask] = asyncio.Queue()
        self.seen_urls: Set[str] = set()
        self.task_results: Dict[str, dict] = {}
        self.session: aiohttp.ClientSession = None
    
    async def start(self):
        self.session = aiohttp.ClientSession()
        await asyncio.gather(
            self.work_processor(),
            self.peer_coordinator()
        )

    async def work_processor(self):
        while True:
            task = await self.work_queue.get()
            if task.url in self.seen_urls:
                continue
                
            try:
                async with self.session.get(task.url) as response:
                    content = await response.text()
                    self.task_results[task.task_id] = {
                        'url': task.url,
                        'status': response.status,
                        'content_length': len(content)
                    }
                    self.seen_urls.add(task.url)
                    
                    # Extract new URLs and create subtasks
                    if task.depth > 0:
                        new_urls = self.extract_urls(content)
                        for url in new_urls:
                            subtask = CrawlTask(url=url, depth=task.depth-1)
                            await self.distribute_task(subtask)
                            
            except Exception as e:
                self.task_results[task.task_id] = {
                    'url': task.url,
                    'error': str(e)
                }
            
            self.work_queue.task_done()

    async def peer_coordinator(self):
        while True:
            # Regularly share task results with peers
            for peer in self.peers:
                try:
                    async with self.session.post(
                        f'{peer}/sync',
                        json={
                            'node_id': self.node_id,
                            'results': self.task_results
                        }
                    ) as response:
                        peer_data = await response.json()
                        self.merge_peer_data(peer_data)
                except Exception:
                    pass
            await asyncio.sleep(5)

    async def distribute_task(self, task: CrawlTask):
        # Simple round-robin task distribution
        target = sorted(list(self.peers))[hash(task.task_id) % len(self.peers)]
        try:
            async with self.session.post(
                f'{target}/task',
                json=task.__dict__
            ) as response:
                if response.status != 200:
                    await self.work_queue.put(task)
        except Exception:
            await self.work_queue.put(task)

    def merge_peer_data(self, peer_data: dict):
        # Merge results from peers while avoiding duplicates
        self.seen_urls.update(peer_data.get('seen_urls', []))
        for task_id, result in peer_data.get('results', {}).items():
            if task_id not in self.task_results:
                self.task_results[task_id] = result

    @staticmethod
    def extract_urls(content: str) -> List[str]:
        # Implement URL extraction from HTML content
        # This is a placeholder - implement proper HTML parsing
        return []

    async def close(self):
        if self.session:
            await self.session.close()