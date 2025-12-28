from __future__ import annotations

"""
inspect_attrs.py

Hard-coded to a SOURCE.

Loops through all auction_listings rows for that source, reads the JSONB
raw_attrs column, and prints a list of:

    ATTRIBUTE_NAME (N unique)
        - value 1
        - value 2
        - ...

Also writes the same output to a markdown file:
    data/<SOURCE>.md
(e.g. data/ebay-motors.md)
"""

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Set, List

from utils.db_schema import get_connection
from utils.logger import get_logger

logger = get_logger(__name__)

# SOURCE = "ebay-consoles"
# SOURCE = "ebay-apple"
# SOURCE = "ebay-actioncams"
# SOURCE = "ebay-retro-pc"
# SOURCE = "ebay-watches"
# SOURCE = "ebay-tools"
# SOURCE = "motomine"
#SOURCE = "ebay-motors"
#SOURCE = "ebay-lego"
SOURCE = "ebay-pokemon"


def _to_str(v: Any) -> str | None:
    """Normalise raw JSON values into a readable string."""
    if v is None:
        return None

    if isinstance(v, (dict, list, tuple, set)):
        try:
            s = json.dumps(v, ensure_ascii=False, sort_keys=True)
        except TypeError:
            s = str(v)
    else:
        s = str(v)

    s = s.strip()
    return s or None


def _build_markdown(source: str, attr_values: Dict[str, Set[str]]) -> str:
    lines: List[str] = []
    lines.append(f"# Attribute values for source: `{source}`")
    lines.append("")
    lines.append(f"Total attributes: **{len(attr_values)}**")
    lines.append("")

    for attr_name in sorted(attr_values.keys()):
        values = sorted(attr_values[attr_name])
        lines.append("---")
        lines.append(f"## {attr_name} ({len(values)} unique)")
        lines.append("")
        for val in values:
            lines.append(f"- `{val}`")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    conn = get_connection()
    attr_values: Dict[str, Set[str]] = defaultdict(set)

    with conn, conn.cursor() as cur:
        logger.info("Loading raw_attrs for source=%s ...", SOURCE)
        cur.execute(
            """
            SELECT raw_attrs
            FROM auction_listings
            WHERE source = %s
              AND raw_attrs IS NOT NULL
            """,
            (SOURCE,),
        )

        rows = cur.fetchall()
        logger.info("Fetched %d rows", len(rows))

        for (raw_attrs,) in rows:
            if not raw_attrs:
                continue

            if isinstance(raw_attrs, str):
                try:
                    data = json.loads(raw_attrs)
                except json.JSONDecodeError:
                    logger.warning(
                        "Could not decode raw_attrs string: %r",
                        raw_attrs[:200],
                    )
                    continue
            else:
                data = raw_attrs

            if not isinstance(data, dict):
                continue

            for key, value in data.items():
                if isinstance(value, (list, tuple, set)):
                    for item in value:
                        s = _to_str(item)
                        if s:
                            attr_values[key].add(s)
                else:
                    s = _to_str(value)
                    if s:
                        attr_values[key].add(s)

    # Console output (unchanged)
    print(f"\n=== Attribute values for source='{SOURCE}' ===\n")

    for attr_name in sorted(attr_values.keys()):
        values = sorted(attr_values[attr_name])
        print("=" * 80)
        print(f"{attr_name} ({len(values)} unique)")
        for val in values:
            print(f" - {val}")
        print()

    # Write markdown to ./data/<SOURCE>.md
    md_content = _build_markdown(SOURCE, attr_values)

    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    data_dir.mkdir(exist_ok=True)

    out_path = data_dir / f"{SOURCE}.md"
    out_path.write_text(md_content, encoding="utf-8")

    logger.info("Wrote markdown: %s", out_path)
    logger.info("Done – printed %d attributes", len(attr_values))


if __name__ == "__main__":
    main()
