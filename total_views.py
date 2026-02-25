from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any, Optional
import pandas as pd

from bigdataloader2 import getData
import numpy as np
from datetime import datetime, timedelta
import pytz

from aiocache import cached # type: ignore
from aiocache.serializers import PickleSerializer  # type: ignore

from databases.psql import engine, schema

from ..models.licenseReduction import ViewedReportsRequest

router = APIRouter()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CACHE_TTL_SECONDS = 86400  # 24 hours
LOOKUP_TTL_SECONDS = 86400  # 24 hours (Spotfire users + employee tables)
REPORT_VIEWS_TTL_SECONDS = 4 * 60 * 60   # 4 hours (per report_path)

LICENSE_COLS = [
    "USER_NAME",
    "USER_EMAIL",
    "LAST_ACTIVITY",
    "ANALYST_FUNCTIONS",
    "NON_ANALYST_FUNCTIONS",
    "ANALYST_PCT",
    "ANALYST_USER_FLAG",
    "ANALYST_THRESHOLD",
    "ANALYST_ACTIONS_PER_DAY",
    "ANALYST_ACTIONS_PER_ACTIVE_DAYS",
    "ACTIVE_DAYS",
]

TZ_CDT = pytz.timezone("America/Chicago")
TZ_UTC = pytz.UTC

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_license_df() -> pd.DataFrame:
    """
    Pull the analyst functions users dataset from PostgreSQL
    (replaces the old S3 CSV load).
    """
    cols_sql = ", ".join([f'"{c}"' for c in LICENSE_COLS])
    sql = f'SELECT {cols_sql} FROM "{schema}".analyst_functions_users'
    df = pd.read_sql_query(sql, con=engine)
    df.columns = [c.strip() for c in df.columns]
    return df


def _get_primary_employee_data() -> pd.DataFrame:
    """
    Primary employee lookup. We include extra identifiers to support
    additional matching passes: bname, nt_id, gad_id.
    Also includes org metadata (cost center / dept / title).
    """
    params = {"data_type": "pageradm_employee_ghr", "MLR": "L"}
    custom_columns = [
        "full_name",
        "smtp",
        "status_name",
        "bname",
        "nt_id",
        "gad_id",
        "cost_center_name",
        "dept_name",
        "title",
    ]
    return getData(params=params, custom_columns=custom_columns)


def _get_fallback_employee_data() -> pd.DataFrame:
    """
    Secondary/fallback employee lookup (full_name, smtp, status_name, org fields).
    Used only when the primary merge fails to resolve FULL_NAME.
    """
    params = {"data_type": "dss_employee_ghr", "MLR": "L"}
    custom_columns = [
        "full_name",
        "smtp",
        "status_name",
        "cost_center_name",
        "dept_name",
        "title",
    ]
    return getData(params=params, custom_columns=custom_columns)


def _partner_to_samsung_email(email: Optional[str]) -> Optional[str]:
    """
    If a license row uses a contractor/partner email but the employee tables
    now contain @samsung.com, we try an alternate email.

    Example:
    someone@partner.samsung.com -> someone@samsung.com
    """
    if not email:
        return email
    e = str(email).strip()
    if "@partner.samsung" in e:
        left = e.split("@", 1)[0]
        return f"{left}@samsung.com"
    return e


def _email_localpart(email: Optional[str]) -> Optional[str]:
    """
    Return the part of an email before '@'. If '@' not present, returns the input.
    """
    if not email:
        return None
    e = str(email).strip()
    if "@" not in e:
        return e
    return e.split("@", 1)[0]


def _fill_missing_from_key(
    merged: pd.DataFrame,
    missing_mask: pd.Series,
    lookup: pd.DataFrame,
    lookup_key: str,
    left_key_series: pd.Series,
) -> pd.DataFrame:
    """
    Fill FULL_NAME + STATUS_NAME + org fields (cost_center_name, dept_name, title)
    for rows where merged[FULL_NAME] is missing, using a lookup table keyed by
    `lookup_key`, matching `left_key_series`.

    - lookup must contain: lookup_key, full_name, status_name, cost_center_name, dept_name, title
    - left_key_series is the values to look up (same index as merged[missing_mask])
    """
    if not missing_mask.any():
        return merged

    needed = {lookup_key, "full_name", "status_name", "cost_center_name", "dept_name", "title"}
    if not needed.issubset(set(lookup.columns)):
        return merged

    lk = lookup[[lookup_key, "full_name", "status_name", "cost_center_name", "dept_name", "title"]].copy()
    lk[lookup_key] = lk[lookup_key].astype(str).str.strip().str.lower()
    lk = lk.dropna(subset=[lookup_key]).drop_duplicates(subset=[lookup_key], keep="first")

    left_keys_norm = left_key_series.astype(str).str.strip().str.lower()

    name_map = lk.set_index(lookup_key)["full_name"].to_dict()
    status_map = lk.set_index(lookup_key)["status_name"].to_dict()
    cc_map = lk.set_index(lookup_key)["cost_center_name"].to_dict()
    dept_map = lk.set_index(lookup_key)["dept_name"].to_dict()
    title_map = lk.set_index(lookup_key)["title"].to_dict()

    merged.loc[missing_mask, "FULL_NAME"] = merged.loc[missing_mask, "FULL_NAME"].fillna(
        left_keys_norm.map(name_map)
    )
    merged.loc[missing_mask, "STATUS_NAME"] = merged.loc[missing_mask, "STATUS_NAME"].fillna(
        left_keys_norm.map(status_map)
    )
    merged.loc[missing_mask, "cost_center_name"] = merged.loc[missing_mask, "cost_center_name"].fillna(
        left_keys_norm.map(cc_map)
    )
    merged.loc[missing_mask, "dept_name"] = merged.loc[missing_mask, "dept_name"].fillna(
        left_keys_norm.map(dept_map)
    )
    merged.loc[missing_mask, "title"] = merged.loc[missing_mask, "title"].fillna(
        left_keys_norm.map(title_map)
    )

    return merged


def _fill_missing_email_from_employee_ids(
    df_in: pd.DataFrame,
    employee_df: pd.DataFrame,
    user_name_col: str,
    email_col: str,
) -> pd.DataFrame:
    """
    For rows where df[email_col] is missing, try to find an email by matching
    df[user_name_col] against employee identifiers (bname, nt_id, gad_id),
    and set df[email_col] = employee_df.smtp.

    This avoids assuming email = user_name + @samsung.com.
    """
    df = df_in.copy()

    if user_name_col not in df.columns:
        return df
    if email_col not in df.columns:
        df[email_col] = None

    missing_email = df[email_col].isna() | (df[email_col].astype(str).str.strip() == "")
    if not missing_email.any():
        return df

    left = df.loc[missing_email, user_name_col].astype(str).str.strip().str.lower()

    emp = employee_df.copy()
    for col in ["smtp", "bname", "nt_id", "gad_id"]:
        if col in emp.columns:
            emp[col] = emp[col].astype(str).str.strip().str.lower()

    if "smtp" not in emp.columns:
        return df

    maps: List[Dict[str, str]] = []

    if "bname" in emp.columns:
        maps.append(
            emp.dropna(subset=["bname", "smtp"])
            .drop_duplicates("bname")
            .set_index("bname")["smtp"]
            .to_dict()
        )
    if "nt_id" in emp.columns:
        maps.append(
            emp.dropna(subset=["nt_id", "smtp"])
            .drop_duplicates("nt_id")
            .set_index("nt_id")["smtp"]
            .to_dict()
        )
    if "gad_id" in emp.columns:
        maps.append(
            emp.dropna(subset=["gad_id", "smtp"])
            .drop_duplicates("gad_id")
            .set_index("gad_id")["smtp"]
            .to_dict()
        )

    found = pd.Series(index=left.index, dtype="object")
    for m in maps:
        if found.isna().all():
            found = left.map(m)
        else:
            found = found.fillna(left.map(m))

    df.loc[missing_email, email_col] = df.loc[missing_email, email_col].fillna(found)

    # normalize final email
    df[email_col] = df[email_col].where(df[email_col].notna(), None)
    if df[email_col].notna().any():
        df[email_col] = df[email_col].astype(str).str.strip().str.lower().replace({"nan": None, "": None})

    return df


# ---------------------------------------------------------------------------
# Cached lookups (big wins: avoid re-pulling same Trino tables per request)
# ---------------------------------------------------------------------------


@cached(ttl=LOOKUP_TTL_SECONDS, serializer=PickleSerializer())
async def get_cached_sf_users() -> pd.DataFrame:
    """
    Spotfire user mapping table (display_name, email, user_name).
    Cached because it is reused across report-views calls.
    """
    params = {"data_type": "spotfire_if2sf_users", "MLR": "T"}
    df = getData(params=params, custom_columns=["display_name", "email", "user_name"])
    if df is None or df.empty:
        return pd.DataFrame(columns=["user_name", "display_name", "email"])

    df = df.copy()
    df["user_name"] = df["user_name"].astype(str).str.strip()
    if "email" in df.columns:
        df["email"] = (
            df["email"]
            .where(df["email"].notna(), None)
            .astype(str)
            .str.strip()
            .str.lower()
            .replace({"nan": None, "": None})
        )
    return df


@cached(ttl=LOOKUP_TTL_SECONDS, serializer=PickleSerializer())
async def get_cached_primary_emp() -> pd.DataFrame:
    """
    Primary employee table cached + normalized once.
    """
    df = _get_primary_employee_data()
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "smtp",
                "bname",
                "nt_id",
                "gad_id",
                "full_name",
                "status_name",
                "cost_center_name",
                "dept_name",
                "title",
            ]
        )

    df = df.copy()

    for col in ["smtp", "bname", "nt_id", "gad_id"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()

    for col in ["full_name", "status_name", "cost_center_name", "dept_name", "title"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"nan": None})

    return df


@cached(ttl=LOOKUP_TTL_SECONDS, serializer=PickleSerializer())
async def get_cached_fallback_emp() -> pd.DataFrame:
    """
    Fallback employee table cached + normalized once.
    """
    df = _get_fallback_employee_data()
    if df is None or df.empty:
        return pd.DataFrame(columns=["smtp", "full_name", "status_name", "cost_center_name", "dept_name", "title"])

    df = df.copy()

    if "smtp" in df.columns:
        df["smtp"] = df["smtp"].astype(str).str.strip().str.lower()

    for col in ["full_name", "status_name", "cost_center_name", "dept_name", "title"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"nan": None})

    return df


def enrich_with_employee_data(
    df_in: pd.DataFrame,
    email_col: str,
    username_col: str,
    primary_emp: Optional[pd.DataFrame] = None,
    fallback_emp: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Employee enrichment pipeline.

    Produces/fills:
      - FULL_NAME
      - STATUS_NAME
      - cost_center_name
      - dept_name
      - title


    Perf change: can accept preloaded/cached employee tables to avoid re-pulling Trino.
    """
    df = df_in.copy()

    out_cols = ["FULL_NAME", "STATUS_NAME", "cost_center_name", "dept_name", "title"]

    # --- Preserve any existing values, but DROP to avoid duplicate columns after merge ---
    existing = pd.DataFrame(index=df.index)
    for c in out_cols:
        if c in df.columns:
            existing[c] = df[c]
        else:
            existing[c] = None

    df.drop(columns=[c for c in out_cols if c in df.columns], inplace=True, errors="ignore")

    # Normalize email/username inputs
    if email_col not in df.columns:
        df[email_col] = None
    df[email_col] = (
        df[email_col]
        .where(df[email_col].notna(), None)
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"nan": None, "": None})
    )

    if username_col not in df.columns:
        df[username_col] = None
    df[username_col] = (
        df[username_col]
        .where(df[username_col].notna(), None)
        .astype(str)
        .str.strip()
        .replace({"nan": None})
    )

    # Internal helper cols
    df["_EMAIL_ALT"] = (
        df[email_col]
        .apply(_partner_to_samsung_email)
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"nan": None, "": None})
    )
    df["_EMAIL_LOCAL"] = df[email_col].apply(_email_localpart)

    # Load primary employee data (prefer injected cached)
    user_data = (primary_emp if primary_emp is not None else _get_primary_employee_data()).copy()

    # Normalize lookup keys (safe even if already normalized)
    for col in ["smtp", "bname", "nt_id", "gad_id"]:
        if col in user_data.columns:
            user_data[col] = user_data[col].astype(str).str.strip().str.lower()

    # Normalize values
    for col in ["full_name", "status_name", "cost_center_name", "dept_name", "title"]:
        if col in user_data.columns:
            user_data[col] = user_data[col].astype(str).str.strip().replace({"nan": None})

    # 1) Primary merge on email -> smtp
    merged = (
        df.merge(
            user_data,
            how="left",
            left_on=email_col,
            right_on="smtp",
            suffixes=("", "_emp"),
        )
        .drop(columns=["smtp"], errors="ignore")
    )

    # --- Capture employee values BEFORE we overlay "existing" columns ---
    emp_full = merged["full_name"] if "full_name" in merged.columns else pd.Series(index=merged.index, dtype="object")
    emp_status = (
        merged["status_name"] if "status_name" in merged.columns else pd.Series(index=merged.index, dtype="object")
    )

    emp_cc = (
        merged["cost_center_name"]
        if "cost_center_name" in merged.columns
        else pd.Series(index=merged.index, dtype="object")
    )
    emp_dept = merged["dept_name"] if "dept_name" in merged.columns else pd.Series(index=merged.index, dtype="object")
    emp_title = merged["title"] if "title" in merged.columns else pd.Series(index=merged.index, dtype="object")

    # --- Now build the output columns: prefer existing values, else employee values ---
    merged["FULL_NAME"] = existing["FULL_NAME"].copy().fillna(emp_full)
    merged["STATUS_NAME"] = existing["STATUS_NAME"].copy().fillna(emp_status)
    merged["cost_center_name"] = existing["cost_center_name"].copy().fillna(emp_cc)
    merged["dept_name"] = existing["dept_name"].copy().fillna(emp_dept)
    merged["title"] = existing["title"].copy().fillna(emp_title)

    # 2) Fallback merge ONLY where FULL_NAME is missing
    missing_mask = merged["FULL_NAME"].isna()
    if missing_mask.any():
        fb = (fallback_emp if fallback_emp is not None else _get_fallback_employee_data()).copy()
        if "smtp" in fb.columns:
            fb["smtp"] = fb["smtp"].astype(str).str.strip().str.lower()

        to_fix = merged.loc[missing_mask].merge(
            fb,
            how="left",
            left_on=email_col,
            right_on="smtp",
            suffixes=("", "_fb"),
        )

        key_series = merged.loc[missing_mask, email_col]

        if "full_name" in to_fix.columns:
            name_map = to_fix.set_index(email_col)["full_name"].to_dict()
            merged.loc[missing_mask, "FULL_NAME"] = merged.loc[missing_mask, "FULL_NAME"].fillna(
                key_series.map(name_map)
            )

        if "status_name" in to_fix.columns:
            status_map = to_fix.set_index(email_col)["status_name"].to_dict()
            merged.loc[missing_mask, "STATUS_NAME"] = merged.loc[missing_mask, "STATUS_NAME"].fillna(
                key_series.map(status_map)
            )

        for col in ["cost_center_name", "dept_name", "title"]:
            if col in to_fix.columns:
                col_map = to_fix.set_index(email_col)[col].to_dict()
                merged.loc[missing_mask, col] = merged.loc[missing_mask, col].fillna(key_series.map(col_map))

    # 3) Partner email repair (still missing + partner email)
    still_missing = merged["FULL_NAME"].isna()
    partner_missing = still_missing & merged[email_col].astype(str).str.contains("@partner.samsung", na=False)

    if partner_missing.any():
        to_fix2 = merged.loc[partner_missing].merge(
            user_data,
            how="left",
            left_on="_EMAIL_ALT",
            right_on="smtp",
            suffixes=("", "_alt"),
        )

        key_series = merged.loc[partner_missing, email_col]

        if "full_name" in to_fix2.columns:
            name_map2 = to_fix2.set_index(email_col)["full_name"].to_dict()
            merged.loc[partner_missing, "FULL_NAME"] = merged.loc[partner_missing, "FULL_NAME"].fillna(
                key_series.map(name_map2)
            )

        if "status_name" in to_fix2.columns:
            status_map2 = to_fix2.set_index(email_col)["status_name"].to_dict()
            merged.loc[partner_missing, "STATUS_NAME"] = merged.loc[partner_missing, "STATUS_NAME"].fillna(
                key_series.map(status_map2)
            )

        for col in ["cost_center_name", "dept_name", "title"]:
            if col in to_fix2.columns:
                col_map2 = to_fix2.set_index(email_col)[col].to_dict()
                merged.loc[partner_missing, col] = merged.loc[partner_missing, col].fillna(key_series.map(col_map2))

    # 4) Additional resolution passes
    missing = merged["FULL_NAME"].isna()
    if missing.any() and "bname" in user_data.columns:
        user_bname = user_data.dropna(subset=["bname"]).copy()
        merged = _fill_missing_from_key(
            merged=merged,
            missing_mask=missing,
            lookup=user_bname,
            lookup_key="bname",
            left_key_series=merged.loc[missing, username_col],
        )

    missing = merged["FULL_NAME"].isna()
    if missing.any() and "nt_id" in user_data.columns:
        user_ntid = user_data.dropna(subset=["nt_id"]).copy()
        merged = _fill_missing_from_key(
            merged=merged,
            missing_mask=missing,
            lookup=user_ntid,
            lookup_key="nt_id",
            left_key_series=merged.loc[missing, username_col],
        )

    missing = merged["FULL_NAME"].isna()
    if missing.any() and "gad_id" in user_data.columns:
        user_gad = user_data.dropna(subset=["gad_id"]).copy()
        merged = _fill_missing_from_key(
            merged=merged,
            missing_mask=missing,
            lookup=user_gad,
            lookup_key="gad_id",
            left_key_series=merged.loc[missing, "_EMAIL_LOCAL"],
        )

    # 5) Final fallback
    final_missing = merged["FULL_NAME"].isna()
    if final_missing.any():
        merged.loc[final_missing, "FULL_NAME"] = "Possibly Terminated"
        merged.loc[final_missing, "STATUS_NAME"] = merged.loc[final_missing, "STATUS_NAME"].fillna("Unknown")
        for col in ["cost_center_name", "dept_name", "title"]:
            merged.loc[final_missing, col] = merged.loc[final_missing, col].fillna("Unknown")

    # Cleanup helper cols and employee raw cols
    merged.drop(columns=["_EMAIL_ALT", "_EMAIL_LOCAL"], inplace=True, errors="ignore")
    merged.drop(columns=["full_name", "status_name"], inplace=True, errors="ignore")

    return merged


# ---------------------------------------------------------------------------
# Cached data builders
# ---------------------------------------------------------------------------


@cached(ttl=CACHE_TTL_SECONDS, serializer=PickleSerializer())
async def get_cached_final_df() -> pd.DataFrame:
    """
    Build the fully-enriched dataset once (per TTL) and cache it:

    1) Load base rows from PostgreSQL (analyst_functions_users)
    2) Compute recommendedAction
    3) Employee enrichment to get FULL_NAME + STATUS_NAME + org fields
       (now uses cached employee tables)
    """
    df = get_license_df()
    df.columns = [c.strip() for c in df.columns]
    df = df.where(df.notna(), None)

    # Normalize email fields early (helps joins)
    if "USER_EMAIL" in df.columns:
        df["USER_EMAIL"] = df["USER_EMAIL"].astype(str).str.strip().str.lower().replace({"nan": None, "": None})
    if "USER_NAME" in df.columns:
        df["USER_NAME"] = df["USER_NAME"].astype(str).str.strip().replace({"nan": None})

    # Numeric conversion (db can still give strings depending on driver/types)
    if "ANALYST_ACTIONS_PER_DAY" in df.columns:
        df["ANALYST_ACTIONS_PER_DAY"] = pd.to_numeric(df["ANALYST_ACTIONS_PER_DAY"], errors="coerce").fillna(0)

    # Compute recommendedAction once
    df["recommendedAction"] = df["ANALYST_ACTIONS_PER_DAY"].apply(lambda x: "Analyst" if float(x) >= 1 else "Consumer")

    # Keep these for debug endpoint parity
    df["USER_EMAIL_ALT"] = df["USER_EMAIL"].apply(_partner_to_samsung_email)
    df["USER_EMAIL_LOCAL"] = df["USER_EMAIL"].apply(_email_localpart)

    for col in ["cost_center_name", "dept_name", "title"]:
        if col not in df.columns:
            df[col] = None

    primary_emp = await get_cached_primary_emp()
    fallback_emp = await get_cached_fallback_emp()

    merged = enrich_with_employee_data(
        df,
        email_col="USER_EMAIL",
        username_col="USER_NAME",
        primary_emp=primary_emp,
        fallback_emp=fallback_emp,
    )

    return merged


@cached(ttl=CACHE_TTL_SECONDS, serializer=PickleSerializer())
async def get_cached_cost_centers_list() -> List[str]:
    """
    Cache the cost center list so the UI dropdown doesn't cause repeated work.
    """
    df = await get_cached_final_df()

    if "cost_center_name" not in df.columns:
        raise HTTPException(status_code=400, detail="Missing 'cost_center_name' after employee merge")

    centers = sorted({str(x).strip() for x in df["cost_center_name"].dropna().tolist() if str(x).strip()})
    return centers


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/cost-centers", response_model=List[str])
async def get_cost_centers() -> List[str]:
    return await get_cached_cost_centers_list()


@router.get("/license-reduction", response_model=List[Dict[str, Any]])
async def get_license_reduction(
    cost_center_name: str = Query(..., description="Exact cost-center name"),
) -> List[Dict[str, Any]]:
    """
    Return a list of records in the exact shape expected by the Next frontend.
    """
    df = await get_cached_final_df()

    if "cost_center_name" not in df.columns:
        raise HTTPException(status_code=400, detail="Missing 'cost_center_name' after employee merge")

    mask = df["cost_center_name"].astype(str).str.strip().eq(cost_center_name.strip())
    filtered = df.loc[mask].copy()

    def safe(v):
        return None if pd.isna(v) else v

    def row_to_ui(r: pd.Series) -> Dict[str, Any]:
        return {
            # UI-visible columns
            "name": safe(r.get("FULL_NAME")),
            "statusName": safe(r.get("STATUS_NAME")),
            "user": safe(r.get("USER_NAME")),
            "email": safe(r.get("USER_EMAIL")),
            "costCenterName": safe(r.get("cost_center_name")),
            "departmentName": safe(r.get("dept_name")),
            "title": safe(r.get("title")),
            "recommendedAction": safe(r.get("recommendedAction")),
            # Extra fields (optional; safe to keep for later UI expansion)
            "lastActivity": safe(r.get("LAST_ACTIVITY")),
            "analystActionsPerDay": float(r.get("ANALYST_ACTIONS_PER_DAY") or 0),
            "analystFunctions": int(r.get("ANALYST_FUNCTIONS") or 0),
            "nonAnalystFunctions": int(r.get("NON_ANALYST_FUNCTIONS") or 0),
            "activeDays": int(r.get("ACTIVE_DAYS") or 0),
            "titleCategory": safe(r.get("TITLE_CATEGORY")),
            "analystPct": safe(r.get("ANALYST_PCT")),
            "analystUserFlag": bool(r.get("ANALYST_USER_FLAG") or False),
            "analystThreshold": safe(r.get("ANALYST_THRESHOLD")),
        }

    return [row_to_ui(row) for _, row in filtered.iterrows()]


@router.get("/license-reduction/missing-names", response_model=List[Dict[str, Any]])
async def get_missing_full_names() -> List[Dict[str, Any]]:
    """
    Debug endpoint: show rows that STILL do not have FULL_NAME after all passes.
    """
    df = await get_cached_final_df()
    missing = df[df["FULL_NAME"] == "Possibly Terminated"].copy()

    def safe(v):
        return None if pd.isna(v) else v

    return [
        {
            "user": safe(r.get("USER_NAME")),
            "email": safe(r.get("USER_EMAIL")),
            "altEmail": safe(r.get("USER_EMAIL_ALT")),
            "emailLocal": safe(r.get("USER_EMAIL_LOCAL")),
            "costCenterName": safe(r.get("cost_center_name")),
            "departmentName": safe(r.get("dept_name")),
            "title": safe(r.get("title")),
            "recommendedAction": safe(r.get("recommendedAction")),
        }
        for _, r in missing.iterrows()
    ]


# ---------------------------------------------------------------------------
# /report-views caching (per report_path)
# ---------------------------------------------------------------------------


def _report_views_cache_key(func, *args, **kwargs) -> str:
    # args[0] should be report_path in our cached worker
    report_path = args[0] if args else kwargs.get("report_path", "")
    return f"report_views:{report_path}"


@cached(ttl=REPORT_VIEWS_TTL_SECONDS, serializer=PickleSerializer(), key_builder=_report_views_cache_key)
async def _get_report_views_cached(report_path: str) -> List[Dict[str, Any]]:
    """
    Cached worker: does the heavy lifting for /report-views.
    Major perf improvements:
      - caches SF users and employee tables (Trino pulls) for LOOKUP_TTL_SECONDS
      - caches report views per report_path for REPORT_VIEWS_TTL_SECONDS
      - avoids DataFrame merge for sf_users: uses dict mapping (fast for small result sets)
      - parses logged_time once
      - single dedupe strategy (email preferred, fallback to user_name)
      - avoids df.fillna("") (expensive, and forces string conversions)
    """
    cutoff_dt = datetime.utcnow() - timedelta(days=30)
    cutoff_dt = cutoff_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_str = cutoff_dt.strftime("%d-%b-%y %I.%M.%S.%f %p")

    df_reports = getData(
        params={
            "data_type": "spotfire_if2sf_actionlog",
            "MLR": "T",
            "log_category": "library%",
            "log_action": ["load_content", "load"],
            "logged_time": cutoff_str,
            "success": "1",
            "arg1": "dxp",
            "id2": report_path,
            "user_name": [
                "SPOTFIRESYSTEM\\automationservices",
                "SPOTFIRESYSTEM\\monitoring",
                "SPOTFIRESYSTEM\\scheduledupdates",
                "SPOTFIREOAUTH2\\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com",
            ],
        },
        custom_columns=["id2", "log_action", "log_category", "logged_time", "user_name", "session_id"],
        custom_operators={"log_category": "like", "user_name": "!", "logged_time": ">="},
    )

    if df_reports is None or df_reports.empty:
        return []

    df_reports = df_reports.copy()

    # Parse once, drop bad times
    df_reports["logged_time"] = pd.to_datetime(df_reports["logged_time"], errors="coerce", utc=True)
    df_reports = df_reports.dropna(subset=["logged_time"])

    # Latest per user_name early (shrinks data ASAP)
    if "user_name" in df_reports.columns:
        df_reports["user_name"] = df_reports["user_name"].astype(str).str.strip()
        df_reports = (
            df_reports.sort_values(["user_name", "logged_time"], ascending=[True, False])
            .drop_duplicates(subset=["user_name"], keep="first")
            .reset_index(drop=True)
        )

    # Cached lookups
    sf_users = await get_cached_sf_users()
    primary_emp = await get_cached_primary_emp()
    fallback_emp = await get_cached_fallback_emp()

    # Map SF user email/display_name instead of merge (faster for small frames)
    if sf_users is not None and not sf_users.empty:
        email_map = sf_users.set_index("user_name")["email"].to_dict() if "email" in sf_users.columns else {}
        display_map = (
            sf_users.set_index("user_name")["display_name"].to_dict() if "display_name" in sf_users.columns else {}
        )
        df_reports["email"] = df_reports["user_name"].map(email_map)
        df_reports["display_name"] = df_reports["user_name"].map(display_map)
    else:
        df_reports["email"] = None
        df_reports["display_name"] = None

    # Fill missing emails via employee ids (cached primary)
    df_reports = _fill_missing_email_from_employee_ids(
        df_in=df_reports,
        employee_df=primary_emp,
        user_name_col="user_name",
        email_col="email",
    )

    # Normalize email once
    df_reports["email"] = (
        df_reports["email"]
        .where(df_reports["email"].notna(), None)
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"nan": None, "": None})
    )

    # Single dedupe strategy: prefer email, fallback to user_name
    df_reports["_dedupe_key"] = df_reports["email"].fillna(df_reports["user_name"])
    df_reports = (
        df_reports.sort_values(["_dedupe_key", "logged_time"], ascending=[True, False])
        .drop_duplicates(subset=["_dedupe_key"], keep="first")
        .drop(columns=["_dedupe_key"])
        .sort_values("logged_time", ascending=False)
        .reset_index(drop=True)
    )

    # Employee enrichment (cached tables injected)
    df_reports = enrich_with_employee_data(
        df_reports,
        email_col="email",
        username_col="user_name",
        primary_emp=primary_emp,
        fallback_emp=fallback_emp,
    )

    
    # --- Dedupe by FULL_NAME (keep latest logged_time) ---
    if "FULL_NAME" in df_reports.columns:
        has_full_name = df_reports["FULL_NAME"].notna()

        # For rows with FULL_NAME: dedupe by FULL_NAME, keep latest logged_time
        df_with_full_name = (
            df_reports.loc[has_full_name]
            .sort_values(["FULL_NAME", "logged_time"], ascending=[True, False])
            .drop_duplicates(subset=["FULL_NAME"], keep="first")
        )

        # For rows without FULL_NAME: keep as-is
        df_no_full_name = df_reports.loc[~has_full_name]

        df_reports = (
            pd.concat([df_with_full_name, df_no_full_name], ignore_index=True)
            .sort_values("logged_time", ascending=False)
            .reset_index(drop=True)
        )
    
    # Keep your debug behavior
    unresolved = df_reports[df_reports["FULL_NAME"] == "Possibly Terminated"]
    if not unresolved.empty:
        print("Users unresolved after employee enrichment:")
        print(unresolved["user_name"].unique())

    # Return JSON-friendly payload without forcing everything to ""
    return df_reports.replace({np.nan: None}).to_dict(orient="records")


@router.post("/report-views")
async def get_report_views(req: ViewedReportsRequest):
    """
    Returns views for a passed report (cached per report_path).
    """
    return await _get_report_views_cached(req.report_path)
