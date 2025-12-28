from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.db_schema import connection

import os
from typing import Dict, List

PPH_MAX_ENDING_HOURS = int(os.getenv("GF_PPH_MAX_ENDING_HOURS", "24"))

# Import the same adapters used by retrieve, but directly from the adapters package
from pipelines.listing.retrieve.adapters.motomine import Adapter as MotoMineAdapter
from pipelines.listing.retrieve.adapters.apple import Adapter as AppleAdapter
from pipelines.listing.retrieve.adapters.consoles import Adapter as ConsolesAdapter
from pipelines.listing.retrieve.adapters.retro_pc import Adapter as RetroPcAdapter
from pipelines.listing.retrieve.adapters.actioncams import Adapter as ActionCamAdapter
from pipelines.listing.retrieve.adapters.watches import Adapter as WatchAdapter
from pipelines.listing.retrieve.adapters.tools import Adapter as ToolAdapter
from pipelines.listing.retrieve.adapters.motors import Adapter as MotorsAdapter
from pipelines.listing.retrieve.adapters.lego import Adapter as LegoAdapter
from pipelines.listing.retrieve.adapters.pokemon import Adapter as PokemonAdapter
from pipelines.listing.retrieve.adapters.samsung import Adapter as SamsungAdapter
from pipelines.listing.retrieve.adapters.headphones import Adapter as HeadphonesAdapter
from pipelines.listing.retrieve.adapters.hondaNc750 import Adapter as Nc750Adapter

logger = get_logger(__name__)

# Map real domain -> adapter class using Adapter.DOMAIN
DOMAIN_ADAPTERS: Dict[str, Any] = {
    MotoMineAdapter.DOMAIN: MotoMineAdapter,
    AppleAdapter.DOMAIN: AppleAdapter,
    ConsolesAdapter.DOMAIN: ConsolesAdapter,
    RetroPcAdapter.DOMAIN: RetroPcAdapter,
    ActionCamAdapter.DOMAIN: ActionCamAdapter,
    WatchAdapter.DOMAIN: WatchAdapter,
    ToolAdapter.DOMAIN: ToolAdapter,
    MotorsAdapter.DOMAIN: MotorsAdapter,
    LegoAdapter.DOMAIN: LegoAdapter,
    PokemonAdapter.DOMAIN: PokemonAdapter,
    SamsungAdapter.DOMAIN: SamsungAdapter,
    HeadphonesAdapter.DOMAIN: HeadphonesAdapter,
    Nc750Adapter.DOMAIN: Nc750Adapter,
}


class PphState(TypedDict, total=False):
    ebay_token: str
    # domain -> list of external_ids to refresh
    items_by_domain: Dict[str, List[str]]
    domains: List[str]
    idx: int
    current_domain: Optional[str]
    results: List[Dict[str, Any]]


# -----------------------------
# DB loader
# -----------------------------
def _load_targets(max_rows: int = 500) -> Dict[str, List[str]]:
    """
    Load 'live' listings that should have price refreshed.

    Tiered refresh strategy:
      - Very soon (ends in <= 1 hour):
            refresh if last_seen_at < now - 10 minutes
      - Soon (ends in 1–24 hours):
            refresh if last_seen_at < now - 120 minutes (2 hours)
      - Far away (ends in > 24 hours):
            refresh if last_seen_at < now - 1440 minutes (24 hours, once per day)
    """
    sql = """
        SELECT
            l.external_id,
            s.domain
        FROM auction_listings AS l
        JOIN sources AS s
          ON s.id = l.source_id
        WHERE l.status = 'live'
          AND COALESCE(l.finalized, FALSE) = FALSE
          AND l.end_time IS NOT NULL
          AND l.end_time > (now() AT TIME ZONE 'utc')
          AND l.last_seen_at IS NOT NULL
          AND (
                -- Very soon: ends within 1 hour -> refresh if older than 10 minutes
                (
                  l.end_time <= (now() AT TIME ZONE 'utc') + interval '1 hour'
                  AND l.last_seen_at < (now() AT TIME ZONE 'utc') - interval '10 minutes'
                )
                OR
                -- Soon: ends in 1–24 hours -> refresh if older than 120 minutes
                (
                  l.end_time >  (now() AT TIME ZONE 'utc') + interval '1 hour'
                  AND l.end_time <= (now() AT TIME ZONE 'utc') + interval '24 hours'
                  AND l.last_seen_at < (now() AT TIME ZONE 'utc') - interval '120 minutes'
                )
                OR
                -- Far away: ends in more than 24 hours -> refresh if older than 1440 minutes (1 day)
                (
                  l.end_time > (now() AT TIME ZONE 'utc') + interval '24 hours'
                  AND l.last_seen_at < (now() AT TIME ZONE 'utc') - interval '1440 minutes'
                )
              )
        ORDER BY l.end_time ASC
        LIMIT %s
    """

    with connection.cursor() as cur:
        cur.execute(sql, (max_rows,))
        rows = cur.fetchall()

    items_by_domain: Dict[str, List[str]] = {}
    for external_id, domain in rows:
        if not external_id or not domain:
            continue
        items_by_domain.setdefault(domain, []).append(external_id)

    total = sum(len(v) for v in items_by_domain.values())
    logger.info(
        "[pph] loaded %s listings across %s domains for price refresh (tiered TTLs)",
        total,
        len(items_by_domain),
    )
    return items_by_domain


# -----------------------------
# Nodes
# -----------------------------
def init_state(state: PphState) -> PphState:
    state.setdefault("results", [])
    state["items_by_domain"] = _load_targets()
    state["domains"] = list(state["items_by_domain"].keys())
    state["idx"] = 0
    state["current_domain"] = None
    return state


def pick_next_domain(state: PphState) -> PphState:
    idx = int(state.get("idx", 0))
    domains = state.get("domains") or []

    if idx >= len(domains):
        state["current_domain"] = None
    else:
        state["current_domain"] = domains[idx]

    return state


def refresh_domain(state: PphState) -> PphState:
    domain = state.get("current_domain")
    if not domain:
        return state

    external_ids = (state.get("items_by_domain") or {}).get(domain, [])
    if not external_ids:
        state.setdefault("results", []).append(
            {"domain": domain, "status": "skipped", "reason": "no_items"}
        )
        state["idx"] = int(state.get("idx", 0)) + 1
        return state

    adapter_cls = DOMAIN_ADAPTERS.get(domain)
    if adapter_cls is None:
        logger.warning("[pph:%s] unknown adapter (no adapter for domain)", domain)
        state.setdefault("results", []).append(
            {"domain": domain, "status": "skipped", "reason": "unknown_adapter"}
        )
        state["idx"] = int(state.get("idx", 0)) + 1
        return state

    adapter = adapter_cls()

    try:
        logger.info(
            "[pph:%s] refreshing %s listings",
            domain,
            len(external_ids),
        )
        # You will implement this on each adapter.
        #
        # Expected behaviour:
        # - For each external_id, call a cheap eBay endpoint (e.g. GetItem/Browse)
        # - Normalise into (row, ph) like fetch_listings_api
        # - Reuse bulk_upsert_auction_listings + bulk_append_price_history
        #
        adapter.refresh_items_price(state["ebay_token"], external_ids)

        state.setdefault("results", []).append(
            {
                "domain": domain,
                "status": "refreshed",
                "count": len(external_ids),
            }
        )
    except AttributeError:
        # Adapter doesn't (yet) implement refresh_items_price
        logger.error(
            "[pph:%s] adapter %s has no refresh_items_price(ebay_token, item_ids)",
            domain,
            adapter_cls.__name__,
        )
        state.setdefault("results", []).append(
            {
                "domain": domain,
                "status": "skipped",
                "reason": "adapter_missing_refresh_items_price",
            }
        )
    except Exception as e:
        logger.warning(
            "[pph:%s] refresh failed: %s\n%s",
            domain,
            e,
            traceback.format_exc(),
        )
        connection.rollback()
        state.setdefault("results", []).append(
            {
                "domain": domain,
                "status": "failed",
                "error": str(e),
            }
        )

    state["idx"] = int(state.get("idx", 0)) + 1
    return state


def should_continue(state: PphState) -> str:
    idx = int(state.get("idx", 0))
    domains = state.get("domains") or []
    return "continue" if idx < len(domains) else "done"


# -----------------------------
# Build graph
# -----------------------------
def build_graph():
    g = StateGraph(PphState)

    g.add_node("init", init_state)
    g.add_node("pick_domain", pick_next_domain)
    g.add_node("refresh_domain", refresh_domain)

    g.set_entry_point("init")
    g.add_edge("init", "pick_domain")
    g.add_edge("pick_domain", "refresh_domain")

    g.add_conditional_edges(
        "refresh_domain",
        should_continue,
        {
            "continue": "pick_domain",
            "done": END,
        },
    )

    return g.compile()


def save_graph_diagram(path: str = "pph_graph.mmd") -> None:
    graph = build_graph()
    g = graph.get_graph()
    mermaid = g.draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)
    logger.info("[pph] wrote graph mermaid to %s", path)


def run(*, ebay_token: str) -> PphState:
    graph = build_graph()
    initial: PphState = {"ebay_token": ebay_token}
    logger.info("[pph] Begin price-per-hour refresh (LangGraph)")
    out: PphState = graph.invoke(initial, config={"recursion_limit": 200})
    logger.info("[pph] End price-per-hour refresh (LangGraph)")
    return out
