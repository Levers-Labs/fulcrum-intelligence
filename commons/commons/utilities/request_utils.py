from urllib.parse import urlparse

from fastapi import Request


def get_referer(request: Request) -> str:
    """
    Get the referer URL from the request headers
    """
    referer = request.headers.get("referer")
    segments = urlparse(referer)
    return f"{segments.scheme}://{segments.netloc}"  # type: ignore
