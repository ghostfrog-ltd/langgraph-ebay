from __future__ import annotations

import json

from pipelines.listing.project_msx125.graph import run


def main() -> None:
    print(json.dumps(run(), indent=2, default=str))


if __name__ == "__main__":
    main()
