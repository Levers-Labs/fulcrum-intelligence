from typing import Any

from slack_sdk import WebClient


class SlackClient:
    """A wrapper around the Slack WebClient providing common functionality"""

    def __init__(self, token: str):
        """Initialize the Slack client with authentication token"""
        self.client = WebClient(token=token)

    def list_channels(self, cursor: str | None = None, limit: int = 100, name: str | None = None) -> dict[str, Any]:
        """
        List channels in the workspace with pagination support and name filtering

        Args:
            cursor: Optional pagination cursor from previous response
            limit: Number of channels to return per page, default 100
            name: Optional channel name filter, case-insensitive partial match

        Returns:
            Dict[str, Any]: Response containing:
                - channels: List of channel objects containing id, name etc.
                - next_cursor: Cursor for fetching next page, None if no more pages
        """
        # Call Slack API to get list of channels with pagination
        response = self.client.conversations_list(cursor=cursor, limit=limit)
        channels = response["channels"]

        # Filter channels by name if name filter is provided
        if name:
            # Convert name to lowercase for case-insensitive comparison
            name = name.lower()
            # Keep only channels whose names contain the search term
            channels = [channel for channel in channels if name in channel["name"].lower()]

        # Filter to only list channels i.e. is_channel is True
        channels = [channel for channel in channels if channel["is_channel"]]

        # Return filtered channels and pagination cursor
        next_cursor = response["response_metadata"].get("next_cursor")

        # based on the filter, we might not get any channels and
        # have next_cursor as we have in code filters
        # in that case, we should rerun function with next_cursor value
        # if not channels and next_cursor:
        #     return self.list_channels(cursor=next_cursor, limit=limit, name=name)

        return {
            "results": channels,
            "next_cursor": next_cursor if next_cursor else None,
        }

    def post_message(
        self,
        channel_id: str,
        text: str | None = None,
        blocks: list[dict[str, Any]] | None = None,
        attachments: list[dict[str, Any]] | None = None,
    ) -> dict:
        """
        Post a message to a Slack channel

        Args:
            channel_id: The channel ID to post to
            text: Optional text content
            blocks: Optional block kit blocks
            attachments: Optional message attachments

        Returns:
            dict: Response containing:
                - ok: True if the message was successfully posted
                - channel: The channel ID the message was posted to
                - ts: The timestamp of the message
        """
        kwargs: dict[str, Any] = {"channel": channel_id}
        if text:
            kwargs["text"] = text
        if blocks:
            kwargs["blocks"] = blocks
        if attachments:
            kwargs["attachments"] = attachments

        response = self.client.chat_postMessage(**kwargs)
        return {
            "ok": response["ok"],
            "channel": response.get("channel"),
            "ts": response.get("ts"),
        }

    def get_channel_info(self, channel_id: str) -> dict[str, Any]:
        """
        Get channel info in the workspace for the given channel id

        Args:
            channel_id: channel id for the channel

        Returns:
            Dict[str, Any]: Response containing:
                - channel: channel objects containing id, name etc.
        """
        # Call Slack API to get channel info
        response = self.client.conversations_info(
            channel=channel_id,
        )
        # Return the channel info
        return response["channel"]
