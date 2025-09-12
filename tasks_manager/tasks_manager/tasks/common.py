def format_delivery_results(delivery_results: list) -> str:
    """Format delivery results for the markdown summary"""
    if not delivery_results:
        return "No delivery details available"

    formatted = []
    for result in delivery_results:
        if not isinstance(result, dict):
            continue
        channel = result.get("channel", "Unknown")
        status = result.get("status", "Unknown")
        error = f" (Error: {result.get('error', 'Unknown error')})" if result.get("error") else ""
        formatted.append(f"- {channel}: {status}{error}")

    return "\n".join(formatted) if formatted else "No valid delivery results available"
