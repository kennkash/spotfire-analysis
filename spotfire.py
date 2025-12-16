# ------------------------------------------------------------
# Spotfire Analyst Utilization + Platform Usage (90-day window)
# ------------------------------------------------------------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from bigdataloader2 import getData
import s2cloudapi.s3api as s3

# -----------------------------
# CONFIG
# -----------------------------
ANALYST_THRESHOLD = 50  # percent threshold used in dashboard

TZ_CDT = pytz.timezone("America/Chicago")
TZ_UTC = pytz.UTC

cutoff_dt = datetime.utcnow() - timedelta(days=90)
cutoff_dt = cutoff_dt.replace(hour=0, minute=0, second=0, microsecond=0)
cutoff_str = cutoff_dt.strftime("%d-%b-%y %I.%M.%S.%f %p")

# Cloud vs Local Desktop IP mapping for auth_pro logins
CLOUD_IPS = {"192.12.345.123", "192.12.345.456", "192.12.345.789"}
LOCAL_IPS = {"105.987.65.432"}

# Users to exclude from reporting
USERNAME_EXCLUDES = [
    "user1", "user2", "user3", "user4", "user5",
    "user6", "user7", "user8", "user9", "user10",
]

# Categories that you want to treat as "analyst-only" for this analysis
ANALYST_CATEGORIES = {
    "analysis_pro",
    "data_connector_pro",
    "info_link",
}

# Categories to exclude entirely (noise/system OR “available regardless of license” AND not useful for determining analyst need)
EXCLUDE_CATEGORIES = [
    # Generic/system noise
    "admin",
    "analysis_as",
    "auth", "auth_as", "auth_pro", "auth_wp",
    "automation_job_as", "automation_task_as",
    "codetrust",
    "dblogging",
    "ems",
    "monitoring", "monitoring_wp", "monitoring_as",
    "routing_rules",
    "scheduled_updates",

    # Categories within license structure that you want to EXCLUDE from analysis
    # (your stated intent: actions possible in both web + desktop regardless of license, or not meaningful for determining Analyst need)
    "file_pro", "file_wp", "file_as",
    "find_pro", "find_wp", "find_as",
    "data_connector_as", "data_connector_wp",
    "datasource_pro", "datasource_wp", "datasource_as",
    "datafunction_pro", "datafunction_wp", "datafunction_as",
    "library", "library_as", "library_wp", "library_pro",
]

# Actions to exclude entirely (common consumer interactions that muddy the “license-needed” signal)
EXCLUDE_ACTIONS = [
    "apply_bookmark",
    "create_comment",
    "export",
    "modify_filter",
    "reset_all_visible_filters",
    "reset_filter",
    "set_page",
    "load_connection",
    "load_source",
]

# Info_link exceptions: these do NOT require Analyst even though info_link is in ANALYST_CATEGORIES
INFO_LINK_NON_ANALYST_ACTIONS = {"get_data", "load_il"}


# -----------------------------
# HELPERS
# -----------------------------
def utc_to_cdt(series: pd.Series) -> pd.Series:
    """Convert UTC datetimes to America/Chicago and format as string."""
    if series.dt.tz is None:
        series = series.dt.tz_localize(TZ_UTC)
    else:
        series = series.dt.tz_convert(TZ_UTC)

    return series.dt.tz_convert(TZ_CDT).dt.strftime("%Y-%m-%d %H:%M:%S")


def categorize_title(title_val: str) -> str:
    """
    Map raw job titles into buckets:
    - Leadership
    - Engineer
    - Tech
    - Other
    """
    if not isinstance(title_val, str) or not title_val.strip():
        return "Other"

    t = title_val.lower()

    leadership_keywords = ["manager", "vp", "director", "supervisor", "lead", "head"]
    engineer_keywords = ["engineer", "eng", "developer", "devops", "architect", "scientist"]
    tech_keywords = ["technician", "tech", "operator", "specialist", "associate", "maintenance"]

    if any(k in t for k in leadership_keywords):
        return "Leadership"
    if any(k in t for k in engineer_keywords):
        return "Engineer"
    if any(k in t for k in tech_keywords):
        return "Tech"
    return "Other"


def classify_pro_platform(ip: str) -> str:
    """Classify auth_pro login machine IP as Cloud vs Local Desktop vs Other."""
    if ip in CLOUD_IPS:
        return "Cloud"
    if ip in LOCAL_IPS:
        return "Local Desktop"
    return "Other"


def normalize_username(u: str) -> str:
    """
    Normalize Spotfire username for nt_id matching.
    Handles DOMAIN\\user formats too.
    """
    if not isinstance(u, str):
        return ""
    u = u.strip()
    # take the tail if it's DOMAIN\user
    if "\\" in u:
        u = u.split("\\")[-1]
    return u.strip().lower()


# ------------------------------------------------------------
# 2. LOAD USERS WITH LAST LOGIN (90 DAYS)
# ------------------------------------------------------------
user_columns = ["email", "last_login", "user_id", "user_name"]

params_login = {
    "data_type": "spotfire_if2sf_users",
    "MLR": "T",
    "last_login": cutoff_str,
    "user_name": USERNAME_EXCLUDES,
}

users_df = getData(
    params=params_login,
    custom_columns=user_columns,
    custom_operators={"last_login": ">=", "user_name": "!"},
)

users_df["last_login"] = pd.to_datetime(
    users_df["last_login"],
    format="%d-%b-%y %I.%M.%S.%f %p",
    utc=True,
)

users = users_df[["user_id", "user_name", "last_login", "email"]].copy()


# ------------------------------------------------------------
# 3. LOAD ACTION LOG (ANALYST VS NON-ANALYST)
# ------------------------------------------------------------
# NOTE: We keep success=1 and remove excluded actions/categories to reduce noise.
df_actions = getData(
    params={
        "data_type": "spotfire_if2sf_actionlog",
        "MLR": "T",
        "success": "1",
        "log_category": EXCLUDE_CATEGORIES,
        "log_action": EXCLUDE_ACTIONS,
        "logged_time": cutoff_str,
        "user_name": [
            r"SPOTFIRESYSTEM\automationservices",
            r"SPOTFIRESYSTEM\monitoring",
            r"SPOTFIRESYSTEM\scheduledupdates",
            r"SPOTFIREOAUTH2\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com",
        ],
    },
    custom_columns=["log_action", "log_category", "user_name", "logged_time"],
    custom_operators={
        "log_category": "!",
        "log_action": "!",
        "logged_time": ">=",
        "user_name": "!",
    },
)

# Ensure logged_time is datetime (UTC)
# If your actionlog uses a different format, update the format string accordingly.
df_actions["logged_time"] = pd.to_datetime(df_actions["logged_time"], utc=True, errors="coerce")

# Flag analyst rows based on categories
df_actions["is_analyst"] = df_actions["log_category"].isin(ANALYST_CATEGORIES)

# Override: info_link actions that are NOT analyst-requiring
mask_info_link_exceptions = (
    (df_actions["log_category"] == "info_link") &
    (df_actions["log_action"].isin(INFO_LINK_NON_ANALYST_ACTIONS))
)
df_actions.loc[mask_info_link_exceptions, "is_analyst"] = False

# ---- Aggregate to per-user counts
action_counts = (
    df_actions.groupby("user_name")["is_analyst"]
    .agg(
        analyst_cnt=lambda s: int(s.sum()),
        non_analyst_cnt=lambda s: int((~s).sum()),
    )
    .reset_index()
)

users = (
    users.merge(action_counts, on="user_name", how="left")
    .fillna({"analyst_cnt": 0, "non_analyst_cnt": 0})
)

# ---- Analyst actions per day (director request)
# We compute: analyst_cnt / active_days_in_window
# active_days_in_window = distinct days user had ANY included (post-exclusion) action rows
df_actions["action_day"] = df_actions["logged_time"].dt.floor("D")

user_active_days = (
    df_actions.groupby("user_name")["action_day"]
    .nunique()
    .reset_index(name="ACTIVE_DAYS")
)

users = users.merge(user_active_days, on="user_name", how="left").fillna({"ACTIVE_DAYS": 0})
users["ANALYST_ACTIONS_PER_DAY"] = np.where(
    users["ACTIVE_DAYS"] == 0,
    0,
    np.round(users["analyst_cnt"] / users["ACTIVE_DAYS"], 4)
)

# ------------------------------------------------------------
# 4. MERGE HR DATA (EMAIL FIRST, THEN NT_ID FALLBACK, DROP NON-MATCHES)
# ------------------------------------------------------------
params_hr = {"data_type": "pageradm_employee_ghr", "MLR": "L"}
user_data = getData(
    params=params_hr,
    custom_columns=["cost_center_name", "dept_name", "smtp", "title", "nt_id"],
    custom_operators={"smtp": "notnull"},
)

# Normalize HR keys
user_data["smtp"] = user_data["smtp"].astype(str).str.strip().str.lower()
user_data["nt_id"] = user_data["nt_id"].astype(str).str.strip().str.lower()

# Ensure unique nt_id to avoid multi-match explosions
user_data = (
    user_data.sort_values("nt_id")
    .drop_duplicates(subset=["nt_id"], keep="last")
)

# Normalize users keys
users["email_norm"] = users["email"].astype(str).str.strip().str.lower()
users["user_name_norm"] = users["user_name"].apply(normalize_username)

# 4a. Merge on email
merge_email = users.merge(
    user_data,
    left_on="email_norm",
    right_on="smtp",
    how="left",
    indicator=True,
)

matched_email = merge_email[merge_email["_merge"] == "both"].copy()
unmatched_email = merge_email[merge_email["_merge"] == "left_only"].copy()

# Clean unmatched to remove HR columns before second merge
cols_to_remove = ["cost_center_name", "dept_name", "title", "smtp", "nt_id", "_merge"]
unmatched_email.drop(columns=[c for c in cols_to_remove if c in unmatched_email.columns], inplace=True)

# 4b. Merge remaining on nt_id
merge_ntid = unmatched_email.merge(
    user_data,
    left_on="user_name_norm",
    right_on="nt_id",
    how="left",
    indicator=True,
)

matched_ntid = merge_ntid[merge_ntid["_merge"] == "both"].copy()
unmatched_final = merge_ntid[merge_ntid["_merge"] == "left_only"].copy()

# Keep only matches; drop non-matches
users = pd.concat(
    [matched_email.drop(columns=["_merge"]), matched_ntid.drop(columns=["_merge"])],
    ignore_index=True
)

# Cleanup helper columns
users.drop(columns=["email_norm", "user_name_norm"], inplace=True, errors="ignore")

print("Matched on email:", len(matched_email))
print("Matched on nt_id:", len(matched_ntid))
print("Dropped (no HR match):", len(unmatched_final))


# ------------------------------------------------------------
# 5. FINAL USER-LEVEL DATAFRAME
# ------------------------------------------------------------
users["LAST_ACTIVITY"] = utc_to_cdt(users["last_login"])

total = users["analyst_cnt"] + users["non_analyst_cnt"]
users["ANALYST_PCT"] = np.where(
    total == 0, 0, np.round((users["analyst_cnt"] / total) * 100, 2)
)

users["ANALYST_USER_FLAG"] = users["ANALYST_PCT"] >= ANALYST_THRESHOLD
users["ANALYST_THRESHOLD"] = ANALYST_THRESHOLD

users["TITLE_CATEGORY"] = users["title"].apply(categorize_title)

final_df = users.rename(
    columns={
        "user_name": "USER_NAME",
        "email": "USER_EMAIL",
        "analyst_cnt": "ANALYST_FUNCTIONS",
        "non_analyst_cnt": "NON_ANALYST_FUNCTIONS",
    }
)[
    [
        "USER_NAME",
        "USER_EMAIL",
        "LAST_ACTIVITY",
        "ANALYST_FUNCTIONS",
        "NON_ANALYST_FUNCTIONS",
        "ANALYST_PCT",
        "ANALYST_USER_FLAG",
        "ANALYST_THRESHOLD",
        "ANALYST_ACTIONS_PER_DAY",
        "ACTIVE_DAYS",
        "cost_center_name",
        "dept_name",
        "title",
        "TITLE_CATEGORY",
    ]
].sort_values("LAST_ACTIVITY", ascending=False)


# ------------------------------------------------------------
# 6. TOP ANALYST FUNCTIONS (ACTION-LEVEL AGGREGATE)
# ------------------------------------------------------------
df_analyst_actions = df_actions[df_actions["is_analyst"]].copy()

top_actions_df = (
    df_analyst_actions.groupby(["log_action", "log_category"])
    .agg(
        TOTAL_USES=("log_action", "size"),
        UNIQUE_USERS=("user_name", "nunique"),
    )
    .reset_index()
    .sort_values("TOTAL_USES", ascending=False)
    .rename(columns={"log_action": "LOG_ACTION", "log_category": "LOG_CATEGORY"})
)


# ------------------------------------------------------------
# 7. MOST VIEWED REPORTS
# ------------------------------------------------------------
df_reports = getData(
    params={
        "data_type": "spotfire_if2sf_actionlog",
        "MLR": "T",
        "log_category": "library%",
        "log_action": ["load_content", "load"],
        "logged_time": cutoff_str,
        "user_name": [
            r"SPOTFIRESYSTEM\automationservices",
            r"SPOTFIRESYSTEM\monitoring",
            r"SPOTFIRESYSTEM\scheduledupdates",
            r"SPOTFIREOAUTH2\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com",
        ],
    },
    custom_columns=["id2", "log_action", "log_category", "logged_time"],
    custom_operators={"log_category": "like", "user_name": "!", "logged_time": ">="},
)

df_report = (
    df_reports.groupby("id2", as_index=False)
    .agg(total_loads=("id2", "size"))
    .sort_values("total_loads", ascending=False)
    .rename(columns={"id2": "report_path"})
    .reset_index(drop=True)
)


# ------------------------------------------------------------
# 8. PLATFORM USAGE: WEB PLAYER vs CLOUD vs LOCAL DESKTOP
# ------------------------------------------------------------
ACTIVE_USERNAMES = set(users["user_name"].unique())

# 8a. Web Player logins (auth_wp)
df_wp_logins = getData(
    params={
        "data_type": "spotfire_if2sf_actionlog",
        "MLR": "T",
        "log_category": "auth_wp",
        "log_action": "login",
        "logged_time": cutoff_str,
        "success": "1",
        "user_name": [
            r"SPOTFIRESYSTEM\automationservices",
            r"SPOTFIRESYSTEM\monitoring",
            r"SPOTFIRESYSTEM\scheduledupdates",
            r"SPOTFIREOAUTH2\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com",
        ],
    },
    custom_columns=["user_name", "machine", "success"],
    custom_operators={"logged_time": ">=", "user_name": "!"},
)

df_wp_logins = df_wp_logins[df_wp_logins["user_name"].isin(ACTIVE_USERNAMES)].copy()
df_wp_logins["platform"] = "Web Player"

# 8b. Desktop/Cloud logins (auth_pro)
df_pro_logins = getData(
    params={
        "data_type": "spotfire_if2sf_actionlog",
        "MLR": "T",
        "log_category": "auth_pro",
        "log_action": "login",
        "logged_time": cutoff_str,
        "success": "1",
        "user_name": [
            r"SPOTFIRESYSTEM\automationservices",
            r"SPOTFIRESYSTEM\monitoring",
            r"SPOTFIRESYSTEM\scheduledupdates",
            r"SPOTFIREOAUTH2\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com",
        ],
    },
    custom_columns=["user_name", "machine", "success"],
    custom_operators={"logged_time": ">=", "user_name": "!"},
)

df_pro_logins = df_pro_logins[df_pro_logins["user_name"].isin(ACTIVE_USERNAMES)].copy()
df_pro_logins["platform"] = df_pro_logins["machine"].astype(str).apply(classify_pro_platform)

# 8c. Combine + count logins per platform (for chart: count of logins per platform)
df_logins_all = pd.concat([df_wp_logins, df_pro_logins], ignore_index=True)

platform_summary_df = (
    df_logins_all.groupby("platform")
    .size()
    .reset_index(name="LOGIN_COUNT")
    .sort_values("LOGIN_COUNT", ascending=False)
)

# Optional detail: per user, per platform
user_platform_logins = (
    df_logins_all.groupby(["user_name", "platform"])
    .size()
    .reset_index(name="LOGIN_COUNT")
)

# Join HR fields to per-user platform logins
user_platform_logins = user_platform_logins.merge(
    users[["user_name", "email", "cost_center_name", "dept_name", "title", "TITLE_CATEGORY"]],
    on="user_name",
    how="left",
)

platform_usage_df = user_platform_logins.rename(
    columns={"user_name": "USER_NAME", "email": "USER_EMAIL"}
)


# ------------------------------------------------------------
# 9. EXPORT ALL CSVs TO S3
# ------------------------------------------------------------
bucket = "spotfire-admin"

def export_csv(df: pd.DataFrame, filename: str):
    s3_path = f"s3://{bucket}/{filename}"
    if s3.chk_file_exist(bucket, filename):
        s3.delete_file(bucket=bucket, key=filename)
    s3.upload_df_as_csv(bucket=bucket, dataframe=df, s3_path=s3_path)
    print("Uploaded:", filename, "rows:", len(df))


export_csv(final_df, "analyst-functions-users.csv")
export_csv(top_actions_df, "analyst-functions-top-actions.csv")
export_csv(df_report, "top-viewed-reports.csv")
export_csv(platform_usage_df, "spotfire-platform-logins-by-user.csv")
export_csv(platform_summary_df, "spotfire-platform-logins-summary.csv")

print("Done.")