import json
import base64
from typing import List, Dict, Any
import aiohttp


class AsyncYpaPubSubPublisher:
    def __init__(
        self,
        project_id: str,
        topic_name: str,
        host: str,
    ):
        self.project_id = project_id
        self.topic_name = topic_name
        self.host = host
        self.session = None

    @property
    def base_url(self) -> str:
        """The base URL for the PubSub."""
        return f"{self.host}/v1"

    @property
    def topic_path(self) -> str:
        """The path to the topic."""
        return f"projects/{self.project_id}/topics/{self.topic_name}"

    async def publish(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Publish a batch of messages to the topic asynchronously."""
        if not self.session:
            self.session = aiohttp.ClientSession()

        url = f"{self.base_url}/{self.topic_path}:publish"
        pubsub_messages = [
            {
                "data": base64.b64encode(json.dumps(msg).encode("utf-8")).decode(
                    "utf-8"
                ),
                "attributes": msg.get("attributes", {}),
            }
            for msg in messages
        ]
        payload = {"messages": pubsub_messages}

        try:
            async with self.session.post(url, json=payload, timeout=10.0) as response:
                response.raise_for_status()
                result = await response.json()
                print(
                    f"Published {len(messages)} messages with IDs: {result.get('messageIds', [])}"
                )
                return result
        except aiohttp.ClientError as e:
            raise Exception(f"Failed to publish messages: {e}")

    async def close(self) -> None:
        """Close the HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None
            print("Publisher session closed.")
