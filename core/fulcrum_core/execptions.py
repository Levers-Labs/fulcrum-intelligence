class AnalysisError(Exception):
    pass


class InsufficientDataError(Exception):
    default_message = "Insufficient data to perform the analysis"

    def __init__(self, message: str | None = None, extra_info: dict | None = None):
        self.message = message or self.default_message
        self.extra_info = extra_info or {}
