"""A&D Prospect Engine — Streamlit Web Dashboard."""

from __future__ import annotations

import io
import json
import csv
import os
from dataclasses import asdict
from datetime import date, datetime
from typing import List

import pandas as pd
import streamlit as st

from prospect_engine.config import EXISTING_PIPELINE, TARGET_STATES
from prospect_engine.enrichment.company_profile import (
    merge_sources,
    enrich_prospect,
    build_outreach_flags,
    filter_by_founded_year,
)
from prospect_engine.sources import sam_gov, usa_spending, sbir, vc_funding
from prospect_engine.utils.logging_setup import configure_logging

configure_logging()

st.set_page_config(
    page_title="A&D Prospect Engine",
    page_icon="\U0001f6f0",
    layout="wide",
)

SNAPSHOT_PATH = "/tmp/prospect_snapshot.json"


# ---------------------------------------------------------------------------
# Snapshot persistence
# ---------------------------------------------------------------------------


def _date_serializer(obj):
    """JSON serializer for date objects."""
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError("Not serializable: {}".format(type(obj)))


def _save_snapshot(data, collected_at):
    """Save prospects + timestamp to JSON file."""
    with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {"collected_at": collected_at, "prospects": data},
            f,
            default=_date_serializer,
        )


def _load_snapshot():
    """Load saved snapshot. Returns (prospects_list, collected_at_str) or (None, None)."""
    if not os.path.exists(SNAPSHOT_PATH):
        return None, None
    try:
        with open(SNAPSHOT_PATH, "r", encoding="utf-8") as f:
            snap = json.load(f)
        return snap["prospects"], snap["collected_at"]
    except (json.JSONDecodeError, KeyError):
        return None, None


# ---------------------------------------------------------------------------
# Signal collection
# ---------------------------------------------------------------------------


def _collect_signals(states_list):
    """Run the full pipeline and save a snapshot. Returns (data, status_messages)."""
    source_results = []
    status = []

    # SAM.gov
    try:
        result = sam_gov.fetch(states=states_list)
        source_results.append(result)
        status.append("SAM.gov: {} companies".format(len(result)))
    except Exception as exc:
        source_results.append([])
        status.append("SAM.gov: failed ({})".format(str(exc)[:60]))

    # USASpending
    try:
        result = usa_spending.fetch(states=states_list)
        source_results.append(result)
        status.append("USASpending: {} companies".format(len(result)))
    except Exception as exc:
        source_results.append([])
        status.append("USASpending: failed ({})".format(str(exc)[:60]))

    # SBIR
    try:
        result = sbir.fetch(states=states_list)
        source_results.append(result)
        status.append("SBIR: {} companies".format(len(result)))
    except Exception as exc:
        source_results.append([])
        status.append("SBIR: failed ({})".format(str(exc)[:60]))

    # VC Funding
    try:
        result = vc_funding.fetch(states=states_list)
        source_results.append(result)
        status.append("VC/Private: {} companies".format(len(result)))
    except Exception as exc:
        source_results.append([])
        status.append("VC/Private: failed ({})".format(str(exc)[:60]))

    prospects = merge_sources(source_results)
    for p in prospects:
        enrich_prospect(p)
    prospects = filter_by_founded_year(prospects)
    for p in prospects:
        build_outreach_flags(p)
    prospects.sort(key=lambda p: p.total_funding, reverse=True)

    data = [asdict(p) for p in prospects]
    collected_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    _save_snapshot(data, collected_at)

    return data, collected_at, status


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_currency(val):
    """Format a number as USD currency string."""
    if val is None or val == 0:
        return ""
    return "${:,.0f}".format(val)


def _make_link(url, label="View"):
    """Create a markdown link if URL is non-empty."""
    if url:
        return "[{}]({})".format(label, url)
    return ""


# ---------------------------------------------------------------------------
# Load saved data
# ---------------------------------------------------------------------------

prospects_data, collected_at = _load_snapshot()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("A&D Prospect Engine")

# Collect / Refresh button
if prospects_data is not None:
    collect_label = "Refresh Signals"
else:
    collect_label = "Collect Signals"

if st.sidebar.button(collect_label, type="primary", use_container_width=True):
    with st.spinner("Collecting funding signals... this may take a minute."):
        prospects_data, collected_at, status = _collect_signals(list(TARGET_STATES))
    for msg in status:
        st.sidebar.text(msg)
    st.rerun()

if prospects_data is None:
    # ------------------------------------------------------------------
    # Empty state
    # ------------------------------------------------------------------
    st.title("A&D Prospect Engine")
    st.caption("No data collected yet")
    st.info(
        "Click **Collect Signals** in the sidebar to fetch funding data "
        "from SAM.gov, USASpending, SBIR, and VC sources."
    )
    st.stop()

# ------------------------------------------------------------------
# Dashboard (data exists)
# ------------------------------------------------------------------

st.sidebar.markdown("---")
st.sidebar.subheader("Filters")

selected_states = st.sidebar.multiselect(
    "States",
    options=TARGET_STATES,
    default=TARGET_STATES,
)

# Source filter
all_sources = set()
for p in prospects_data:
    all_sources.update(p.get("data_sources", []))
all_sources = sorted(all_sources)

selected_sources = st.sidebar.multiselect(
    "Data Sources",
    options=all_sources,
    default=all_sources,
)

# Funding range
all_funding = [p["total_funding"] for p in prospects_data]
if all_funding:
    max_fund = max(all_funding)
    if max_fund > 0:
        funding_range = st.sidebar.slider(
            "Total Funding Range",
            min_value=0.0,
            max_value=float(max_fund),
            value=(0.0, float(max_fund)),
            format="$%,.0f",
        )
    else:
        funding_range = (0.0, 0.0)
else:
    funding_range = (0.0, 0.0)

# Apply filters
filtered = [
    p
    for p in prospects_data
    if (not selected_states or p.get("state", "") in selected_states)
    and any(s in p.get("data_sources", []) for s in selected_sources)
    and funding_range[0] <= p["total_funding"] <= funding_range[1]
]


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("A&D Prospect Engine")
st.caption("Last updated: {}".format(collected_at))

# Metrics
total = len(filtered)
flagged = sum(1 for p in filtered if p.get("outreach_flags"))
pipeline_count = sum(
    1
    for p in filtered
    if any(name.lower() in p["company_name"].lower() for name in EXISTING_PIPELINE)
)
total_funding = sum(p["total_funding"] for p in filtered)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Prospects", total)
col2.metric("Flagged for Outreach", flagged)
col3.metric("In Pipeline", pipeline_count)
col4.metric("Total Funding", _format_currency(total_funding))


# ---------------------------------------------------------------------------
# Main Table
# ---------------------------------------------------------------------------

st.subheader("Prospects by Total Funding")

if filtered:
    table_rows = []
    for p in filtered:
        sbir_str = ""
        if any(
            [
                p["sbir_phase_i_count"],
                p["sbir_phase_ii_count"],
                p["sbir_phase_iii_count"],
            ]
        ):
            sbir_str = "I:{} II:{} III:{}".format(
                p["sbir_phase_i_count"],
                p["sbir_phase_ii_count"],
                p["sbir_phase_iii_count"],
            )

        is_pipeline = any(
            name.lower() in p["company_name"].lower() for name in EXISTING_PIPELINE
        )

        table_rows.append(
            {
                "Company": (
                    "{} *".format(p["company_name"])
                    if is_pipeline
                    else p["company_name"]
                ),
                "State": p["state"],
                "Contracts": p["contract_count"] or "",
                "SBIR": sbir_str,
                "VC Raised": _format_currency(p["total_vc_raised"]),
                "Total Funding": _format_currency(p["total_funding"]),
                "Flags": len(p.get("outreach_flags", [])) or "",
                "Sources": ", ".join(p.get("data_sources", [])),
            }
        )

    df = pd.DataFrame(table_rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No prospects found matching the current filters.")


# ---------------------------------------------------------------------------
# Company Detail Drill-Down
# ---------------------------------------------------------------------------

st.subheader("Company Details")

if filtered:
    company_names = [p["company_name"] for p in filtered]
    selected_company = st.selectbox("Select a company", company_names)

    if selected_company:
        company = next(p for p in filtered if p["company_name"] == selected_company)

        # Contract Awards
        contracts = company.get("contract_awards", [])
        if contracts:
            with st.expander(
                "Contract Awards ({})".format(len(contracts)), expanded=True
            ):
                contract_rows = []
                for a in contracts:
                    contract_rows.append(
                        {
                            "Source": a.get("source", ""),
                            "Agency": a.get("awarding_agency", ""),
                            "Amount": _format_currency(a.get("obligation_amount", 0)),
                            "Date": a.get("signed_date", "") or "",
                            "Description": (a.get("description", "") or "")[:80],
                            "Link": _make_link(a.get("source_url", "")),
                        }
                    )
                st.dataframe(
                    pd.DataFrame(contract_rows),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Link": st.column_config.LinkColumn(
                            "Link", display_text="View"
                        ),
                    },
                )

        # SBIR Awards
        sbir_awards = company.get("sbir_awards", [])
        if sbir_awards:
            with st.expander(
                "SBIR/STTR Awards ({})".format(len(sbir_awards)), expanded=True
            ):
                sbir_rows = []
                for a in sbir_awards:
                    sbir_rows.append(
                        {
                            "Phase": a.get("phase", ""),
                            "Program": a.get("program", ""),
                            "Agency": a.get("agency", ""),
                            "Title": (a.get("award_title", "") or "")[:60],
                            "Amount": _format_currency(a.get("award_amount", 0)),
                            "Date": a.get("award_date", "") or "",
                            "Link": _make_link(a.get("source_url", "")),
                        }
                    )
                st.dataframe(
                    pd.DataFrame(sbir_rows),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Link": st.column_config.LinkColumn(
                            "Link", display_text="View"
                        ),
                    },
                )

        # VC Rounds
        vc_rounds = company.get("vc_rounds", [])
        if vc_rounds:
            with st.expander(
                "VC / Private Rounds ({})".format(len(vc_rounds)), expanded=True
            ):
                vc_rows = []
                for r in vc_rounds:
                    vc_rows.append(
                        {
                            "Round": r.get("round_type", ""),
                            "Amount": _format_currency(r.get("amount_usd", 0)),
                            "Lead Investor": r.get("lead_investor", ""),
                            "Date": r.get("announced_date", "") or "",
                            "Link": _make_link(r.get("source_url", "")),
                        }
                    )
                st.dataframe(
                    pd.DataFrame(vc_rows),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Link": st.column_config.LinkColumn(
                            "Link", display_text="View"
                        ),
                    },
                )

        # No funding data
        if not contracts and not sbir_awards and not vc_rounds:
            st.info("No individual funding records available for this company.")


# ---------------------------------------------------------------------------
# Outreach Flags
# ---------------------------------------------------------------------------

flagged_prospects = [p for p in filtered if p.get("outreach_flags")]
if flagged_prospects:
    st.subheader("Outreach Flags — Action Required")
    flag_rows = []
    for p in flagged_prospects:
        for flag in p["outreach_flags"]:
            flag_rows.append({"Company": p["company_name"], "Flag": flag})
    st.dataframe(pd.DataFrame(flag_rows), use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Downloads
# ---------------------------------------------------------------------------

st.sidebar.markdown("---")
st.sidebar.subheader("Export")

if filtered:
    # CSV download
    csv_buffer = io.StringIO()
    csv_columns = [
        "company_name",
        "uei",
        "state",
        "city",
        "contract_count",
        "total_contract_obligation",
        "sbir_phase_i_count",
        "sbir_phase_ii_count",
        "sbir_phase_iii_count",
        "total_sbir_amount",
        "total_vc_raised",
        "total_funding",
        "data_sources",
        "outreach_flags",
    ]
    writer = csv.DictWriter(csv_buffer, fieldnames=csv_columns)
    writer.writeheader()
    for p in filtered:
        writer.writerow(
            {
                "company_name": p["company_name"],
                "uei": p.get("uei", ""),
                "state": p.get("state", ""),
                "city": p.get("city", ""),
                "contract_count": p.get("contract_count", 0),
                "total_contract_obligation": p.get("total_contract_obligation", 0),
                "sbir_phase_i_count": p.get("sbir_phase_i_count", 0),
                "sbir_phase_ii_count": p.get("sbir_phase_ii_count", 0),
                "sbir_phase_iii_count": p.get("sbir_phase_iii_count", 0),
                "total_sbir_amount": p.get("total_sbir_amount", 0),
                "total_vc_raised": p.get("total_vc_raised", 0),
                "total_funding": p.get("total_funding", 0),
                "data_sources": ", ".join(p.get("data_sources", [])),
                "outreach_flags": "; ".join(p.get("outreach_flags", [])),
            }
        )
    st.sidebar.download_button(
        "Download CSV",
        csv_buffer.getvalue(),
        file_name="prospects_{}.csv".format(date.today().isoformat()),
        mime="text/csv",
    )

    # JSON download
    json_str = json.dumps(filtered, indent=2, default=_date_serializer)
    st.sidebar.download_button(
        "Download JSON",
        json_str,
        file_name="prospects_{}.json".format(date.today().isoformat()),
        mime="application/json",
    )
