How can I excute code similar to get_cached_final_df() in get_report_views() if user_names are unsuccesful when merging on data? I would want to do the same code within get_cached_final_df() to get to get FULL_NAME + STATUS_NAME + org fields... if the merge IS successful, I would just need to merge email onto smtp to get STATUS_NAME and the org fields.. if not, I would need to find the email with the user_name and go from there to get the rest



from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any, Optional
import pandas as pd

from bigdataloader2 import getData
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz


from aiocache import cached # type: ignore
from aiocache.serializers import PickleSerializer  # type: ignore

from databases.psql import engine, schema

from ..models.licenseReduction import (
    ViewedReportsRequest
)


router = APIRouter()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CACHE_TTL_SECONDS = 86400  # 24 hours

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


# def utc_to_cdt(series: pd.Series) -> pd.Series:
#     """Convert UTC datetimes to America/Chicago and format as string."""
#     if series.dt.tz is None:
#         series = series.dt.tz_localize(TZ_UTC)
#     else:
#         series = series.dt.tz_convert(TZ_UTC)

#     return series.dt.tz_convert(TZ_CDT).dt.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Cached data builders
# ---------------------------------------------------------------------------


@cached(ttl=CACHE_TTL_SECONDS, serializer=PickleSerializer())
async def get_cached_final_df() -> pd.DataFrame:
    """
    Build the fully-enriched dataset once (per TTL) and cache it:

    1) Load base rows from PostgreSQL (analyst_functions_users)
    2) Compute recommendedAction
    3) Primary merge on USER_EMAIL -> smtp to get FULL_NAME + STATUS_NAME + org fields
    4) Fallback merge for remaining missing FULL_NAME (also fills org fields)
    5) Partner email repair (partner -> samsung) for still-missing (also fills org fields)
    6) Additional passes for still-missing:
        a) bname  -> USER_NAME
        b) nt_id  -> USER_NAME
        c) gad_id -> localpart(USER_EMAIL)
    """
    # 1) Load base data from Postgres
    df = get_license_df()
    df.columns = [c.strip() for c in df.columns]
    df = df.where(df.notna(), None)

    # Normalize email fields early (helps joins)
    if "USER_EMAIL" in df.columns:
        df["USER_EMAIL"] = df["USER_EMAIL"].astype(str).str.strip().str.lower()
    if "USER_NAME" in df.columns:
        df["USER_NAME"] = df["USER_NAME"].astype(str).str.strip()

    # Numeric conversion (db can still give strings depending on driver/types)
    if "ANALYST_ACTIONS_PER_DAY" in df.columns:
        df["ANALYST_ACTIONS_PER_DAY"] = pd.to_numeric(
            df["ANALYST_ACTIONS_PER_DAY"], errors="coerce"
        ).fillna(0)

    # Compute recommendedAction once
    df["recommendedAction"] = df["ANALYST_ACTIONS_PER_DAY"].apply(
        lambda x: "Analyst" if float(x) >= 1 else "Consumer"
    )

    # Partner repair candidate email (used later if needed)
    df["USER_EMAIL_ALT"] = (
        df["USER_EMAIL"]
        .apply(_partner_to_samsung_email)
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # Email local-part (for final matching against gad_id)
    df["USER_EMAIL_LOCAL"] = df["USER_EMAIL"].apply(_email_localpart)

    # Ensure org fields exist so downstream fillna logic doesn't KeyError
    for col in ["cost_center_name", "dept_name", "title"]:
        if col not in df.columns:
            df[col] = None

    # 2) Primary employee lookup
    user_data = _get_primary_employee_data().copy()

    # Normalize primary lookup keys
    for col in ["smtp", "bname", "nt_id", "gad_id"]:
        if col in user_data.columns:
            user_data[col] = user_data[col].astype(str).str.strip().str.lower()

    # Normalize org fields in employee data (optional, but helps consistency)
    for col in ["cost_center_name", "dept_name", "title", "full_name", "status_name"]:
        if col in user_data.columns:
            user_data[col] = user_data[col].astype(str).str.strip()

    # 3) Primary merge on email
    merged = (
        df.merge(
            user_data,
            how="left",
            left_on="USER_EMAIL",
            right_on="smtp",
            suffixes=("", "_emp"),
        )
        .drop(columns=["smtp"], errors="ignore")
        .rename(
            columns={
                "full_name": "FULL_NAME",
                "status_name": "STATUS_NAME",
                "cost_center_name": "cost_center_name",
                "dept_name": "dept_name",
                "title": "title",
            }
        )
    )

    # If merge produced duplicate org columns (because df already had them),
    # keep the merged values and drop extras.
    for col in ["cost_center_name", "dept_name", "title"]:
        alt = f"{col}_emp"
        if alt in merged.columns:
            merged[col] = merged[col].fillna(merged[alt])
            merged.drop(columns=[alt], inplace=True)

    # 4) Fallback merge ONLY where FULL_NAME is missing
    missing_mask = merged["FULL_NAME"].isna()
    if missing_mask.any():
        fallback_emp = _get_fallback_employee_data().copy()

        if "smtp" in fallback_emp.columns:
            fallback_emp["smtp"] = fallback_emp["smtp"].astype(str).str.strip().str.lower()

        to_fix = merged.loc[missing_mask].merge(
            fallback_emp,
            how="left",
            left_on="USER_EMAIL",
            right_on="smtp",
            suffixes=("", "_fb"),
        )

        # Fill missing FULL_NAME/STATUS_NAME from fallback merge
        name_map = to_fix.set_index("USER_EMAIL")["full_name"].to_dict()
        status_map = to_fix.set_index("USER_EMAIL")["status_name"].to_dict()

        merged.loc[missing_mask, "FULL_NAME"] = merged.loc[missing_mask, "FULL_NAME"].fillna(
            merged.loc[missing_mask, "USER_EMAIL"].map(name_map)
        )
        merged.loc[missing_mask, "STATUS_NAME"] = merged.loc[missing_mask, "STATUS_NAME"].fillna(
            merged.loc[missing_mask, "USER_EMAIL"].map(status_map)
        )

        # Also fill org fields from fallback merge
        for col in ["cost_center_name", "dept_name", "title"]:
            if col in to_fix.columns:
                col_map = to_fix.set_index("USER_EMAIL")[col].to_dict()
                merged.loc[missing_mask, col] = merged.loc[missing_mask, col].fillna(
                    merged.loc[missing_mask, "USER_EMAIL"].map(col_map)
                )

    # 5) Partner email repair (still missing + partner email)
    still_missing = merged["FULL_NAME"].isna()
    partner_missing = still_missing & merged["USER_EMAIL"].astype(str).str.contains(
        "@partner.samsung", na=False
    )

    if partner_missing.any():
        to_fix2 = merged.loc[partner_missing].merge(
            user_data,
            how="left",
            left_on="USER_EMAIL_ALT",
            right_on="smtp",
            suffixes=("", "_alt"),
        )

        name_map2 = to_fix2.set_index("USER_EMAIL")["full_name"].to_dict()
        status_map2 = to_fix2.set_index("USER_EMAIL")["status_name"].to_dict()

        merged.loc[partner_missing, "FULL_NAME"] = merged.loc[partner_missing, "FULL_NAME"].fillna(
            merged.loc[partner_missing, "USER_EMAIL"].map(name_map2)
        )
        merged.loc[partner_missing, "STATUS_NAME"] = merged.loc[partner_missing, "STATUS_NAME"].fillna(
            merged.loc[partner_missing, "USER_EMAIL"].map(status_map2)
        )

        # Also fill org fields from partner repair merge
        for col in ["cost_center_name", "dept_name", "title"]:
            if col in to_fix2.columns:
                col_map2 = to_fix2.set_index("USER_EMAIL")[col].to_dict()
                merged.loc[partner_missing, col] = merged.loc[partner_missing, col].fillna(
                    merged.loc[partner_missing, "USER_EMAIL"].map(col_map2)
                )

    # 6) Additional resolution passes for anything STILL missing FULL_NAME
    #    a) bname -> USER_NAME
    missing = merged["FULL_NAME"].isna()
    if missing.any() and "bname" in user_data.columns and "USER_NAME" in merged.columns:
        user_bname = user_data.dropna(subset=["bname"]).copy()
        merged = _fill_missing_from_key(
            merged=merged,
            missing_mask=missing,
            lookup=user_bname,
            lookup_key="bname",
            left_key_series=merged.loc[missing, "USER_NAME"],
        )

    #    b) nt_id -> USER_NAME
    missing = merged["FULL_NAME"].isna()
    if missing.any() and "nt_id" in user_data.columns and "USER_NAME" in merged.columns:
        user_ntid = user_data.dropna(subset=["nt_id"]).copy()
        merged = _fill_missing_from_key(
            merged=merged,
            missing_mask=missing,
            lookup=user_ntid,
            lookup_key="nt_id",
            left_key_series=merged.loc[missing, "USER_NAME"],
        )

    #    c) gad_id -> localpart(USER_EMAIL)
    missing = merged["FULL_NAME"].isna()
    if (
        missing.any()
        and "gad_id" in user_data.columns
        and "USER_EMAIL_LOCAL" in merged.columns
    ):
        user_gad = user_data.dropna(subset=["gad_id"]).copy()
        merged = _fill_missing_from_key(
            merged=merged,
            missing_mask=missing,
            lookup=user_gad,
            lookup_key="gad_id",
            left_key_series=merged.loc[missing, "USER_EMAIL_LOCAL"],
        )

    # ------------------------------------------------------------
    # Final fallback: any rows STILL missing FULL_NAME
    # are likely terminated / not found in HR datasets.
    # ------------------------------------------------------------
    final_missing = merged["FULL_NAME"].isna()

    if final_missing.any():
        merged.loc[final_missing, "FULL_NAME"] = "Possibly Terminated"
        merged.loc[final_missing, "STATUS_NAME"] = merged.loc[
            final_missing, "STATUS_NAME"
        ].fillna("Unknown")

        # If org fields are still missing, fill with something consistent
        for col in ["cost_center_name", "dept_name", "title"]:
            merged.loc[final_missing, col] = merged.loc[final_missing, col].fillna("Unknown")

    return merged


@cached(ttl=CACHE_TTL_SECONDS, serializer=PickleSerializer())
async def get_cached_cost_centers_list() -> List[str]:
    """
    Cache the cost center list so the UI dropdown doesn't cause repeated work.
    """
    df = await get_cached_final_df()

    if "cost_center_name" not in df.columns:
        raise HTTPException(
            status_code=400, detail="Missing 'cost_center_name' after employee merge"
        )

    centers = sorted(
        {
            str(x).strip()
            for x in df["cost_center_name"].dropna().tolist()
            if str(x).strip()
        }
    )
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
        raise HTTPException(
            status_code=400, detail="Missing 'cost_center_name' after employee merge"
        )

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
    
@router.post("/report-views")
async def get_report_views(req: ViewedReportsRequest):
    """
    Returns views for a passed report
    
    """
    
    # 90-day cutoff
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
        'success': '1',
        'arg1': 'dxp',
        "id2": req.report_path,
        "user_name": ["SPOTFIRESYSTEM\\automationservices", "SPOTFIRESYSTEM\monitoring", "SPOTFIRESYSTEM\scheduledupdates", "SPOTFIREOAUTH2\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com"],

    },
    custom_columns=["id2", "log_action", "log_category", "logged_time", 'user_name', 'session_id'],
    custom_operators={"log_category": "like", "user_name": "!", "logged_time": ">="},
    )
    
    params = {'data_type': 'spotfire_if2sf_users',
          'MLR': 'T'}
    custom_columns = ['display_name', 'email', 'user_name']
    data = getData(params=params, custom_columns=custom_columns)

    # Merge df_reports with user data to get display_name and email
    df_reports = df_reports.merge(
        data,
        on='user_name',
        how='left',
        suffixes=('', '_user')
    )

    # Print usernames that did not merge successfully (missing display_name or email)
    unmerged_users = df_reports[df_reports['display_name'].isna() | df_reports['email'].isna()]
    if not unmerged_users.empty:
        print("Usernames that did not merge successfully:")
        print(unmerged_users['user_name'].unique())
    else:
        print("All usernames merged successfully")
