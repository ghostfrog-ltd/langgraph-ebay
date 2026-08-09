from __future__ import annotations

from typing import Any, Optional

from pipelines.listing.attributes.mk import normalise_model

from .ebay_base import EbayAdapterBase


class Adapter(EbayAdapterBase):
    DOMAIN = "ebay-industrial-automation"

    CATEGORY_IDS = [
        None,  # exact seeded searches outperform broad industrial categories
    ]

    SALE_TYPE = ["bin", "auction"]
    SEARCH_QUERY = "industrial automation"
    SEARCH_QUERIES = [
        "Siemens SIMATIC PLC",
        "Siemens S7-1500 CPU",
        "Siemens HMI panel",
        "Allen Bradley PLC",
        "Allen Bradley PanelView",
        "Omron PLC CPU",
        "Mitsubishi servo drive",
        "Pilz safety controller",
        "Beckhoff PLC",
        "B&R industrial PC",
        "Control Techniques drive",
        "Schneider Electric PLC",
    ]

    def _model_key_for(self, title: str) -> Optional[str]:
        key = normalise_model(title=title, attrs={}, source=self.DOMAIN)
        return None if key == "unknown" else key

    def _is_relevant(self, row: dict[str, Any]) -> bool:
        title = (row.get("title") or "").lower()
        if any(
            term in title
            for term in (
                "manual",
                "book",
                "catalogue",
                "cable",
                "lead",
                "connector",
                "enclosure",
                "cabinet",
                "control cabinet",
                "repair service",
                "service only",
                "training",
                "software only",
                "license only",
                "licence only",
                "generic switch",
                "network switch",
                "complete machinery",
                "machine only",
                "freight",
                "collection only",
                "faulty",
                "untested",
                "spares",
                "for parts",
                "for repair",
                "job lot",
                "bundle",
            )
        ):
            return False
        return bool(row.get("model_key"))
