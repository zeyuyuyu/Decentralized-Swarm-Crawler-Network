import asyncio
import json
import random
from typing import Dict, Set, List
from dataclasses import dataclass
from datetime import datetime

@dataclass
class WorkItem:
    url: str
    created_at: datetime
    completed_at: datetime = None
    assigned_to: str = None

class CrawlerNode:
    def __init__(self, node_id: str, initial_peers: List[str]):
        self.node_id = node_id
        self.peers = set(initial_peers)
        self.work_items: Dict[str, WorkItem] = {}
        self.seen_urls: Set[str] = set()
        self.gossip_interval = 5  # seconds

    async def start(self):
        await asyncio.gather(
            self.gossip_loop(),
            self.work_loop()
        )

    async def gossip_loop(self):
        while True:
            # Select random subset of peers to gossip with
            gossip_peers = random.sample(
                list(self.peers), 
                min(3, len(self.peers))
            )
            
            for peer in gossip_peers:
                try:
                    await self.sync_with_peer(peer)
                except Exception as e:
                    print(f"Error syncing with {peer}: {e}")
                    
            await asyncio.sleep(self.gossip_interval)

    async def sync_with_peer(self, peer: str):
        # Share work items and get peer's work items
        peer_items = await self.send_work_items(peer, self.work_items)
        
        # Merge peer's work items with local state
        self.merge_work_items(peer_items)

    async def send_work_items(self, peer: str, items: Dict[str, WorkItem]) -> Dict[str, WorkItem]:
        # In real implementation, this would use network calls
        # Simplified for example
        return {}

    def merge_work_items(self, peer_items: Dict[str, WorkItem]):
        for url, item in peer_items.items():
            if url not in self.work_items:
                self.work_items[url] = item
            else:
                # Keep most recent version
                if item.completed_at and not self.work_items[url].completed_at:
                    self.work_items[url] = item

    async def work_loop(self):
        while True:
            # Find unclaimed work
            available_work = [
                url for url, item in self.work_items.items()
                if not item.assigned_to and not item.completed_at
            ]

            if available_work:
                url = random.choice(available_work)
                await self.process_url(url)

            await asyncio.sleep(1)

    async def process_url(self, url: str):
        # Claim the work
        self.work_items[url].assigned_to = self.node_id

        try:
            # Simulate crawling
            await asyncio.sleep(random.uniform(1, 3))
            
            # Mark as completed
            self.work_items[url].completed_at = datetime.now()
            self.seen_urls.add(url)

        except Exception as e:
            # On failure, release the claim
            self.work_items[url].assigned_to = None
            print(f"Error processing {url}: {e}")

    def add_work(self, url: str):
        if url not in self.work_items:
            self.work_items[url] = WorkItem(
                url=url,
                created_at=datetime.now()
            )

    def get_work_status(self) -> Dict[str, dict]:
        return {
            url: {
                "assigned_to": item.assigned_to,
                "completed": bool(item.completed_at),
                "created_at": item.created_at.isoformat()
            }
            for url, item in self.work_items.items()
        }
