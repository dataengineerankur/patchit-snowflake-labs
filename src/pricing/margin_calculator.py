"""Gross margin and pricing analytics for Snowflake reporting pipelines."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MARGIN_ALERT_THRESHOLD = 10.0
HIGH_MARGIN_THRESHOLD = 60.0


@dataclass
class ProductMarginRow:
    product_id: str
    product_name: str
    category: str
    total_revenue: float
    total_cost: float
    gross_margin_pct: float = 0.0
    margin_band: str = "unknown"


@dataclass
class MarginReportSummary:
    report_date: str
    total_products: int
    avg_gross_margin_pct: float
    products_below_threshold: int
    rows: List[ProductMarginRow] = field(default_factory=list)


def compute_gross_margin_pct(revenue: float, cost: float) -> float:
    """Compute gross margin percentage given revenue and cost figures.

    Gross margin = (revenue - cost) / revenue * 100, expressed as a percentage.
    Used for product-level profitability reporting across all SKUs.
    """
    if revenue == 0:
        return 0.0
    return round((revenue - cost) / revenue * 100, 2)


def classify_margin_band(margin_pct: float) -> str:
    """Assign a qualitative band label based on gross margin percentage."""
    if margin_pct < 0:
        return "negative"
    if margin_pct < MARGIN_ALERT_THRESHOLD:
        return "low"
    if margin_pct < HIGH_MARGIN_THRESHOLD:
        return "moderate"
    return "high"


def enrich_margin_row(row: Dict[str, Any]) -> ProductMarginRow:
    """Convert a raw data row dict into a typed, enriched ProductMarginRow."""
    revenue = float(row["total_revenue"])
    cost = float(row["total_cost"])
    margin_pct = compute_gross_margin_pct(revenue, cost)
    return ProductMarginRow(
        product_id=str(row["product_id"]),
        product_name=str(row.get("product_name", "")),
        category=str(row.get("category", "uncategorized")),
        total_revenue=revenue,
        total_cost=cost,
        gross_margin_pct=margin_pct,
        margin_band=classify_margin_band(margin_pct),
    )


def compute_summary_stats(rows: List[ProductMarginRow]) -> Dict[str, float]:
    """Aggregate margin statistics across a set of enriched product rows."""
    if not rows:
        return {"avg_margin_pct": 0.0, "below_threshold_count": 0.0}
    avg = round(sum(r.gross_margin_pct for r in rows) / len(rows), 2)
    below = sum(1 for r in rows if r.gross_margin_pct < MARGIN_ALERT_THRESHOLD)
    return {"avg_margin_pct": avg, "below_threshold_count": float(below)}


def run_margin_report(
    raw_rows: List[Dict[str, Any]],
    report_date: str,
    alert_on_low_margin: bool = True,
) -> MarginReportSummary:
    """Process a batch of product revenue/cost rows and produce a margin report.

    Designed to run as a Snowflake task or dbt post-hook. Each row must contain
    product_id, total_revenue, and total_cost fields.
    """
    enriched_rows: List[ProductMarginRow] = []
    errors = 0

    for row in raw_rows:
        try:
            row["gross_margin_pct"] = compute_gross_margin_pct(row["total_revenue"], row["total_cost"])
            enriched = enrich_margin_row(row)
            enriched_rows.append(enriched)

            if alert_on_low_margin and enriched.gross_margin_pct < MARGIN_ALERT_THRESHOLD:
                logger.warning(
                    "Low margin alert: product=%s margin=%.2f%%",
                    enriched.product_id,
                    enriched.gross_margin_pct,
                )
        except Exception as exc:
            errors += 1
            logger.error("Failed to compute margin for row %s: %s", row.get("product_id"), exc)

    stats = compute_summary_stats(enriched_rows)

    logger.info(
        "Margin report %s: %d products processed, %d errors, avg_margin=%.2f%%",
        report_date,
        len(enriched_rows),
        errors,
        stats["avg_margin_pct"],
    )

    return MarginReportSummary(
        report_date=report_date,
        total_products=len(enriched_rows),
        avg_gross_margin_pct=stats["avg_margin_pct"],
        products_below_threshold=int(stats["below_threshold_count"]),
        rows=enriched_rows,
    )
