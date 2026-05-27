from __future__ import annotations

import json

from .graph import run


def main() -> None:
    print(json.dumps(run({}), indent=2, default=str))


if __name__ == "__main__":
    main()
