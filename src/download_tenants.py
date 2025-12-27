#!/usr/bin/env python3
"""
Download Tenant A (USAspending) samples AND auto-generate tenant schema.yaml
from the official Data Dictionary endpoint (document.headers/rows).

Creates:
- customer_samples/tenant_A/raw/awards_YYYY-MM.jsonl
- customer_samples/tenant_A/contracts.csv
- customer_schemas/tenant_A/schema.yaml

Run:
  python -m pip install requests pyyaml
  python src/download_tenants.py --tenant tenant_A --start 2024-01-01 --end 2024-01-31
"""

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
import yaml  # pip install pyyaml

# ---------- paths ----------
REPO_ROOT = Path(__file__).resolve().parents[1]  # assumes this file is in src/
SAMPLES_DIR = REPO_ROOT / "customer_samples"
SCHEMAS_DIR = REPO_ROOT / "customer_schemas"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def month_key(start: str, end: str) -> str:
    try:
        s = dt.date.fromisoformat(start)
        e = dt.date.fromisoformat(end)
        if s.year == e.year and s.month == e.month:
            return f"{s.year}-{s.month:02d}"
    except Exception:
        pass
    return f"{start}_to_{end}"

def write_jsonl(path: Path, records: Iterable[Dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# ---------- USAspending endpoints ----------
USASPENDING_SEARCH_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
USASPENDING_DICT_URL   = "https://api.usaspending.gov/api/v2/references/data_dictionary/"

# Fields to request for **Contracts** (per spec, kept conservative to avoid 422s)
USAS_FIELDS = [
    # Base subset
    "Award ID",
    "Recipient Name",
    "Recipient UEI",
    "Awarding Agency",
    # Contract-specific
    "Start Date",
    "End Date",
    "Award Amount",
    # You can add later once working:
    # "Awarding Sub Agency", "Contract Award Type", "NAICS", "PSC",
]

# Minimal fallback (if 422 occurs)
USAS_FIELDS_FALLBACK = [
    "Award ID",
    "Recipient Name",
    "Start Date",
    "End Date",
    "Award Amount",
    "Awarding Agency",
]

# ---------- Data Dictionary parsing (document.headers + document.rows) ----------

def fetch_data_dictionary_document() -> Dict:
    """GET the Data Dictionary and return the top-level 'document' object."""
    resp = requests.get(USASPENDING_DICT_URL, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("document") or {}

def build_dictionary_index_from_document(doc: Dict) -> Dict[str, Dict]:
    """
    Build a case-insensitive index of dictionary rows.

    The API returns:
      document.headers: [{raw:"element", display:"Element"}, ...]
      document.rows:    [ ["Element", "Definition", ..., "award_element", ..., "legacy_award_element"], ... ]

    We map each row into a dict using headers[].raw as keys, then index by:
      - 'element'                (Schema Data Label)
      - 'award_element'          (current award file column slug)
      - 'legacy_award_element'   (legacy award file column slug)
    """
    headers = doc.get("headers") or []
    rows = doc.get("rows") or []

    # map column index -> header raw key (e.g., 'element', 'definition', 'award_element', ...)
    idx_to_key: Dict[int, str] = {}
    for i, h in enumerate(headers):
        raw = (h or {}).get("raw")
        if isinstance(raw, str) and raw.strip():
            idx_to_key[i] = raw

    index: Dict[str, Dict] = {}

    def add_index(key: Optional[str], rowmap: Dict):
        if not key or not isinstance(key, str):
            return
        index[key.lower()] = rowmap

    for row in rows:
        if not isinstance(row, list):
            continue
        rowmap: Dict[str, Optional[str]] = {}
        for i, val in enumerate(row):
            raw_key = idx_to_key.get(i)
            if raw_key:
                rowmap[raw_key] = val

        add_index(rowmap.get("element"), rowmap)
        add_index(rowmap.get("award_element"), rowmap)
        add_index(rowmap.get("legacy_award_element"), rowmap)

    return index

def lookup_dict_entry(dd_index: Dict[str, Dict], field_name: str) -> Optional[Dict]:
    """
    Find a dictionary row for a field by:
    - exact field name (e.g., 'Start Date')
    - snake_case variant (e.g., 'start_date')
    """
    candidates = [field_name, field_name.replace(" ", "_")]
    for c in candidates:
        hit = dd_index.get(c.lower())
        if hit:
            return hit
    return None

def best_guess_type(value) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        t = value.strip()
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt.datetime.strptime(t[:19], fmt)
                return "datetime"
            except Exception:
                pass
        for fmt in ("%Y-%m-%d",):
            try:
                dt.datetime.strptime(t[:10], fmt)
                return "date"
            except Exception:
                pass
        return "string"
    return "string"

# ---------- HTTP helpers with 422 handling ----------

class Usaspending422(Exception):
    pass

def _post_spending_by_award(body: dict) -> dict:
    print(f"DEBUG: Sending request body: {json.dumps(body, indent=2)}")
    resp = requests.post(USASPENDING_SEARCH_URL, json=body, timeout=60)
    print(f"DEBUG: Response status: {resp.status_code}")
    if resp.status_code == 422:
        try:
            print("USAspending 422 response:", resp.json())
        except Exception:
            print("USAspending 422 response (text):", resp.text[:800])
        raise Usaspending422("Unprocessable Entity from spending_by_award")
    if resp.status_code != 200:
        print(f"DEBUG: Response text: {resp.text[:800]}")
    resp.raise_for_status()
    return resp.json()

# ---------- Tenant A (contracts via spending_by_award) ----------

def usaspending_fetch_awards(start: str, end: str, limit: int = 500) -> List[Dict]:
    """
    Fetch contract awards (A-D) between dates.
    Uses conservative fields; retries with a minimal set if validation fails.
    """
    page, all_rows = 1, []
    base_body = {
        "subawards": False,
        "limit": limit,
        "sort": "Start Date",  # must be a returned field
        "order": "asc",
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "time_period": [{"start_date": start, "end_date": end}],
            # IMPORTANT: do not include date_type here; it's not required and can trigger 422
        },
    }

    use_fields = USAS_FIELDS[:]  # primary attempt
    fallback_used = False

    while True:
        body = dict(base_body, page=page, fields=use_fields)
        try:
            payload = _post_spending_by_award(body)
        except Usaspending422:
            if fallback_used:
                # Already using fallback; re-raise
                raise
            # Retry once with a minimal set
            use_fields = USAS_FIELDS_FALLBACK[:]
            fallback_used = True
            body = dict(base_body, page=page, fields=use_fields)
            payload = _post_spending_by_award(body)

        rows = payload.get("results") or []
        if not rows:
            break
        all_rows.extend(rows)
        page += 1

    return all_rows

def write_contracts_csv(rows: List[Dict], csv_path: Path) -> None:
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "contract_id",
            "award_id",
            "date_signed",
            "period_start",
            "period_end",
            "value_amount",
            "value_currency",
            "buyer_party_name",
            "supplier_name",
            "supplier_uei",
            # keep optional columns here even if not requested yet
            "contract_award_type",
            "naics",
            "psc",
        ])
        for r in rows:
            award_id = r.get("Award ID") or r.get("generated_internal_id") or ""
            contract_id = award_id  # PIID not guaranteed here

            # Accept both friendly and PoP labels
            start_dt = r.get("Start Date") or r.get("Period of Performance Start Date") or ""
            end_dt   = r.get("End Date")   or r.get("Period of Performance Current End Date") or ""
            date_signed = start_dt  # reasonable proxy

            value = r.get("Award Amount")
            value_str = "" if value is None else str(value)

            buyer_name = r.get("Awarding Agency") or ""
            supplier   = r.get("Recipient Name") or ""
            supplier_uei = r.get("Recipient UEI") or ""

            w.writerow([
                contract_id,
                award_id,
                date_signed,
                start_dt,
                end_dt,
                value_str,
                "USD",
                buyer_name,
                supplier,
                supplier_uei,
                r.get("Contract Award Type") or "",
                r.get("NAICS") or "",
                r.get("PSC") or "",
            ])

def generate_schema_yaml_from_usaspending(rows: List[Dict], schema_path: Path) -> None:
    """
    Build schema.yaml using the official Data Dictionary "document" format.
    Includes every field we requested, plus the alternate PoP labels we may see.
    """
    doc = fetch_data_dictionary_document()
    dd_index = build_dictionary_index_from_document(doc)

    # fields we want to document
    candidate_fields = set(USAS_FIELDS) | {
        "Period of Performance Start Date",
        "Period of Performance Current End Date",
    }

    # sample-based type hints
    sample_values: Dict[str, Optional[object]] = {k: None for k in candidate_fields}
    for r in rows[:200]:
        for k in candidate_fields:
            sample_values[k] = sample_values.get(k) or r.get(k)

    fields_yaml = []
    for name in sorted(candidate_fields):
        hit = lookup_dict_entry(dd_index, name)
        if hit:
            definition = (hit.get("definition") or "").strip()
            dtype = best_guess_type(sample_values.get(name))
            source = "USAspending Data Dictionary"
        else:
            definition = ""
            dtype = best_guess_type(sample_values.get(name))
            source = "USAspending (inferred)"
        fields_yaml.append({
            "name": name,
            "type": dtype or "string",
            "required": False,
            "description": definition,
            "source": source,
        })

    out = {
        "version": "0.1.0",
        "source": "usaspending.gov",
        "endpoint": "/api/v2/search/spending_by_award/",
        "dictionary_metadata": {
            "total_rows": doc.get("metadata", {}).get("total_rows"),
            "total_columns": doc.get("metadata", {}).get("total_columns"),
            "download_location": doc.get("metadata", {}).get("download_location"),
        },
        "notes": "Auto-generated from USAspending Data Dictionary (document.headers/rows) + sampled fields used by the Tenant A downloader.",
        "fields": fields_yaml,
    }

    schema_path.parent.mkdir(parents=True, exist_ok=True)
    with schema_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(out, f, sort_keys=False, allow_unicode=True)

def tenant_A_download(start: str, end: str, limit: int = 500) -> Tuple[Path, Path, Path]:
    """
    Download Tenant A data and write schema.yaml.
    Returns (jsonl_path, csv_path, schema_path).
    """
    tenant = "tenant_A"
    raw_dir = SAMPLES_DIR / tenant / "raw"
    ensure_dir(raw_dir)
    csv_dir = SAMPLES_DIR / tenant
    ensure_dir(csv_dir)

    rows = usaspending_fetch_awards(start, end, limit=limit)

    mk = month_key(start, end)
    jsonl_path = raw_dir / f"awards_{mk}.jsonl"
    write_jsonl(jsonl_path, rows)

    csv_path = csv_dir / "contracts.csv"
    write_contracts_csv(rows, csv_path)

    schema_path = SCHEMAS_DIR / tenant / "schema.yaml"
    generate_schema_yaml_from_usaspending(rows, schema_path)

    return jsonl_path, csv_path, schema_path

# ---------- CLI ----------
TENANT_DOWNLOADERS = {"tenant_A": tenant_A_download}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True, choices=TENANT_DOWNLOADERS.keys())
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=100, help="Page size (USAspending max ~500)")
    args = parser.parse_args()

    jsonl_path, csv_path, schema_path = TENANT_DOWNLOADERS[args.tenant](
        args.start, args.end, limit=args.limit
    )
    print(f"Saved raw JSONL: {jsonl_path}")
    print(f"Saved CSV:       {csv_path}")
    print(f"Saved schema:    {schema_path}")

if __name__ == "__main__":
    main()
