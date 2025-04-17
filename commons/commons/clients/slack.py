from typing import Any

from slack_sdk import WebClient


class SlackClient:
    """A wrapper around the Slack WebClient providing common functionality"""

    def __init__(self, token: str):
        """Initialize the Slack client with authentication token"""
        self.client = WebClient(token=token)

    def list_channels(
        self, cursor: str | None = None, limit: int = 100, name: str | None = None, include_users: bool = False
    ) -> dict[str, Any]:
        """
        List channels in the workspace with pagination support and name filtering.
        Optionally includes users formatted as channels when include_users=True.

        Args:
            cursor: Optional pagination cursor from previous response
            limit: Number of channels to return per page, default 100
            name: Optional channel name filter, case-insensitive partial match
            include_users: Whether to include users in the results

        Returns:
            Dict[str, Any]: Response containing:
                - results: List of channel objects containing id, name etc.
                - next_cursor: Cursor for fetching next page, None if no more pages
        """
        results = []

        # Get channels
        response = self.client.conversations_list(
            cursor=cursor, limit=limit, types=["public_channel", "private_channel", "mpim", "im"]
        )
        channels = response["channels"]

        # Filter channels by name if name filter is provided
        if name:
            name_lower = name.lower()
            channels = [channel for channel in channels if name_lower in channel["name"].lower()]

        results.extend(channels)
        next_cursor = response["response_metadata"].get("next_cursor")

        # Get users if include_users is True
        if include_users:
            user_results = self._fetch_users(limit=limit, name=name)
            results.extend(user_results.get("results", []))

        return {
            "results": results,
            "next_cursor": next_cursor if next_cursor else None,
        }

    def _fetch_users(self, cursor: str | None = None, limit: int = 100, name: str | None = None) -> dict[str, Any]:
        """
        Internal method to fetch users from Slack API and format them as channels

        Args:
            cursor: Optional pagination cursor from previous response
            limit: Number of users to return per page, default 100
            name: Optional name filter for users

        Returns:
            Dict[str, Any]: Response with users formatted as channels
        """
        # Call Slack API to get list of users with pagination
        response = self.client.users_list(cursor=cursor, limit=limit)
        users = response["members"]

        # Filter by name if provided
        if name:
            name_lower = name.lower()
            users = [
                user
                for user in users
                if (name_lower in user.get("name", "").lower() or name_lower in user.get("real_name", "").lower())
            ]

        # Format users as channel objects efficiently
        formatted_users = [
            {
                "id": user["id"],
                "name": user.get("name", ""),
                "is_channel": False,
                "is_group": False,
                "is_dm": True,
                "is_private": True,
                "real_name": user.get("real_name", ""),
                "is_bot": user.get("is_bot", False),
                "is_user": True,  # Add an explicit flag to identify users
            }
            for user in users
            if not user.get("deleted", False)  # Skip deleted users
        ]

        return {
            "results": formatted_users,
            "next_cursor": None,  # We don't use pagination for users in the combined endpoint
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
