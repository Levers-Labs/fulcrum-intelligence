from pathlib import Path


class Paths:
    # query_manager
    ROOT_DIR: Path = Path(__file__).parent.parent
    BASE_DIR: Path = ROOT_DIR / "core"
