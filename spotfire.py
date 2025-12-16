# ------------------------------------------------------------
# 0. IMPORTS & SETUP
# ------------------------------------------------------------
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from bigdataloader2 import getData
import s2cloudapi.s3api as s3

# Adjustable threshold (percent)
ANALYST_THRESHOLD = 50  # change to 40, 60, etc. as needed

TZ_CDT = pytz.timezone("America/Chicago")
TZ_UTC = pytz.UTC

# 90-day cutoff
cutoff_dt = datetime.utcnow() - timedelta(days=90)
cutoff_dt = cutoff_dt.replace(hour=0, minute=0, second=0, microsecond=0)
cutoff_str = cutoff_dt.strftime("%d-%b-%y %I.%M.%S.%f %p")


def utc_to_cdt(series):
    """Convert UTC datetimes to America/Chicago and format as string."""
    if series.dt.tz is None:
        series = series.dt.tz_localize(TZ_UTC)
    else:
        series = series.dt.tz_convert(TZ_UTC)

    return series.dt.tz_convert(TZ_CDT).dt.strftime("%Y-%m-%d %H:%M:%S")


def categorize_title(title_val: str) -> str:
    """
    Map raw job titles into three buckets:
    - Engineer
    - Tech
    - Other
    """
    if not isinstance(title_val, str):
        return "Other"

    t = title_val.lower()
    
    # Leadership-ish roles
    leadership_keywords = [
        "manager", "vp", "director", "supervisor",
        "tr"
    ]

    # Engineer-ish roles
    engineer_keywords = [
        "engineer", "eng", "developer", "devops",
        "architect", "scientist", "data analyst"
    ]

    # Technician / tech / operator type roles
    tech_keywords = [
        "technician", "tech", "operator", "specialist",
        "associate", "analyst", "maintenance"
    ]
    
    if any(k in t for k in leadership_keywords):
        return "Leadership"

    if any(k in t for k in engineer_keywords):
        return "Engineer"
    if any(k in t for k in tech_keywords):
        return "Tech"

    return "Other"


# ------------------------------------------------------------
# 1. SETTINGS
# ------------------------------------------------------------
USERNAME_EXCLUDES = [
"bjeon4",
"schang",
"jsmith",
"ychoi8027",
"dgentry9748",
"cellingsworth3979",
"silkroad.park",
"jtaylor",
"bregan",
"joberbeck",
"ehameister",
"ekerr",
"rlee2",
"jlevy",
"spotfire_automation",
"admin",
"admin.license"
]
ANALYST_CATEGORIES = {
    "analysis_pro",
    "data_connector_pro",
    "info_link",
    # "datafunction_pro", # no other actions besides execute
    # "datasource_pro",
    # "file_pro",
    # "find_pro",
    # "library_pro",
}

EXCLUDE_CATEGORIES = [
    #Generic Categories
    "admin", 
    "analysis_as", 
    "auth", "auth_as", "auth_pro", "auth_wp",
    "automation_job_as", "automation_task_as", 
    "codetrust", 
    "dblogging",
    "ems", 
    "library", "library_as", "library_wp", "library_pro" 
    "monitoring", "monitoring_wp","monitoring_as"
    "routing_rules", 
    "scheduled_updates", 
    
    #Categories within license structure
    "file_pro", "file_wp", 
    "find_pro", "find_wp", 
    "data_connector_as"
    "datasource_pro", "datasource_wp", "datasource_as",  # no other actions besides execute
    "datafunction_pro", "datafunction_wp", 'datafunction_as'  # no other actions besides execute
]

EXCLUDE_ACTIONS = [
    "apply_bookmark", "create_comment", "export", "modify_filter", "reset_all_visible_filters", "reset_filter", "set_page", "load_connection", "load_source"
]
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
df_actions = getData(
    params={
        "data_type": "spotfire_if2sf_actionlog",
        "MLR": "T",
        'success': '1', # action was executed successfully
        "log_category": EXCLUDE_CATEGORIES,
        "log_action": EXCLUDE_ACTIONS,
        "logged_time": cutoff_str,
        "user_name": ["SPOTFIRESYSTEM\\automationservices", "SPOTFIRESYSTEM\monitoring", "SPOTFIRESYSTEM\scheduledupdates", "SPOTFIREOAUTH2\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com"],
    },
    custom_columns=["log_action", "log_category", "user_name"],
    custom_operators={
        "log_category": "!",
        "log_action": "!",
        "logged_time": ">=",
        "user_name": "!",
    },
)

# Flag each row as analyst / non-analyst
df_actions["is_analyst"] = df_actions["log_category"].isin(ANALYST_CATEGORIES)

# Override: Certain info_link actions should *not* count as analyst actions
mask_info_link_exceptions = (
(df_actions["log_category"] == "info_link") &
(df_actions["log_action"].isin(["get_data", "load_il"]))) 
# | (df_actions["log_category"] == "datafunction_pro") &
# (df_actions["log_action"] == 'execute'))


df_actions.loc[mask_info_link_exceptions, "is_analyst"] = False

# Aggregate to per-user counts
action_counts = (
    df_actions.groupby("user_name")["is_analyst"]
    .agg(
        analyst_cnt=lambda s: s.sum(),
        non_analyst_cnt=lambda s: (~s).sum(),
    )
    .reset_index()
)

users = (
    users.merge(action_counts, on="user_name", how="left")
    .fillna({"analyst_cnt": 0, "non_analyst_cnt": 0})
)


# ------------------------------------------------------------
# 4. MERGE HR DATA (EMAIL FIRST, THEN NT_ID FALLBACK, DROPPING NON-MATCHES)
# ------------------------------------------------------------
params_hr = {"data_type": "pageradm_employee_ghr", "MLR": "L"}
user_data = getData(
params=params_hr,
custom_columns=["cost_center_name", "dept_name", "smtp", "title", "nt_id"],
custom_operators={"smtp": "notnull"},
)

# Make nt_id safe and unique
user_data["nt_id"] = user_data["nt_id"].astype(str).str.strip().str.lower()
user_data = (
user_data.sort_values("nt_id")
.drop_duplicates(subset=["nt_id"], keep="last")
)

# Normalize Spotfire usernames for comparing to nt_id
users["user_name_norm"] = users["user_name"].astype(str).str.lower().str.strip()

# ---- 4a. Merge on email (smtp)
merge_email = users.merge(
user_data,
left_on="email",
right_on="smtp",
how="left",
indicator=True
)

matched_email = merge_email[merge_email["_merge"] == "both"].copy()
unmatched_email = merge_email[merge_email["_merge"] == "left_only"].copy()

# Clean up unmatched (remove partially merged HR columns)
cols_to_remove = ["cost_center_name", "dept_name", "title", "smtp", "nt_id", "_merge"]
unmatched_email.drop(columns=[c for c in cols_to_remove if c in unmatched_email.columns],
inplace=True)

# ---- 4b. Merge remaining rows on nt_id
merge_ntid = unmatched_email.merge(
user_data,
left_on="user_name_norm",
right_on="nt_id",
how="left",
indicator=True
)

matched_ntid = merge_ntid[merge_ntid["_merge"] == "both"].copy()
unmatched_final = merge_ntid[merge_ntid["_merge"] == "left_only"].copy()

# ---- 4c. Keep ONLY rows that matched on email OR nt_id
combined = pd.concat([matched_email.drop(columns=["_merge"]),
matched_ntid.drop(columns=["_merge"])],
ignore_index=True)

# ---- 4d. Drop users that fail BOTH merges
users = combined.copy()

# ---- 4e. Optional cleanup
users.drop(columns=["user_name_norm"], inplace=True)


print("Matched on email:", len(matched_email))
print("Matched on nt_id:", len(matched_ntid))
print("Dropped (no HR match):", len(unmatched_final))

# ------------------------------------------------------------
# 5. FINAL USER-LEVEL DATAFRAME
# ------------------------------------------------------------
# Last activity in local time string
users["LAST_ACTIVITY"] = utc_to_cdt(users["last_login"])

# Analyst percentage
total = users["analyst_cnt"] + users["non_analyst_cnt"]
users["ANALYST_PCT"] = np.where(
    total == 0, 0, np.round((users["analyst_cnt"] / total) * 100, 2)
)

# Threshold flag
users["ANALYST_USER_FLAG"] = users["ANALYST_PCT"] >= ANALYST_THRESHOLD
users["ANALYST_THRESHOLD"] = ANALYST_THRESHOLD

# Title category
users["TITLE_CATEGORY"] = users["title"].apply(categorize_title)

# Final user-level dataframe (one row per user)
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
        "cost_center_name",
        "dept_name",
        "title",
        "TITLE_CATEGORY",
    ]
].sort_values("LAST_ACTIVITY", ascending=False)


# ------------------------------------------------------------
# 6. TOP ANALYST FUNCTIONS (ACTION-LEVEL AGGREGATE)
# ------------------------------------------------------------
# Only analyst rows
df_analyst_actions = df_actions[df_actions["is_analyst"]].copy()

top_actions_df = (
    df_analyst_actions.groupby(["log_action", "log_category"])
    .agg(
        total_uses=("log_action", "size"),
        unique_users=("user_name", "nunique"),
    )
    .reset_index()
    .sort_values("total_uses", ascending=False)
)

# Rename columns for clarity in Spotfire
top_actions_df = top_actions_df.rename(
    columns={
        "log_action": "LOG_ACTION",
        "log_category": "LOG_CATEGORY",
        "total_uses": "TOTAL_USES",
        "unique_users": "UNIQUE_USERS",
    }
)

# ------------------------------------------------------------
# 7. MOST VIEWED REPORTS
# ------------------------------------------------------------
params = {'data_type': 'spotfire_if2sf_actionlog',
            'MLR': 'T',
            'log_category': 'library%',
            'log_action': ['load_content', 'load'],
            'user_name': ["SPOTFIRESYSTEM\\automationservices", "SPOTFIRESYSTEM\monitoring", "SPOTFIRESYSTEM\scheduledupdates", "SPOTFIREOAUTH2\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com"],
            'logged_time': cutoff_str}
custom_columns = ['id2', 'log_action', 'log_category', 'logged_time']
custom_operators = {'log_category': 'like', 'user_name': '!', "logged_time": ">=",}
df_reports = getData(params=params, custom_columns=custom_columns, custom_operators=custom_operators)


df_report = (
    df_reports
    .groupby('id2', as_index=False)                     # keep the column
    .agg(total_loads=('id2', 'size'))                   # count per id2
    .sort_values('total_loads', ascending=False)       # ORDER BY total_loads DESC
    .rename(columns={'id2': 'report_path'})             # alias like SELECT
    .reset_index(drop=True)                             # tidy index
)


# ------------------------------------------------------------
# 8. PLATFORM USAGE: WEB PLAYER vs CLOUD vs LOCAL DESKTOP
# ------------------------------------------------------------

# Active usernames (last 90 days)
ACTIVE_USERNAMES = set(users["user_name"].unique())

CLOUD_IPS = {"192.30.106.119", "192.30.106.141", "192.30.106.142"}
LOCAL_IPS = {"105.195.16.243"}

# 8a. Web Player logins (auth_wp, success = 1)
df_wp_logins = getData(
    params={
        "data_type": "spotfire_if2sf_actionlog",
        "MLR": "T",
        "log_category": "auth_wp",
        "log_action": "login",
        "logged_time": cutoff_str,
        "success": "1",
        "user_name": ["SPOTFIRESYSTEM\\automationservices", "SPOTFIRESYSTEM\monitoring", "SPOTFIRESYSTEM\scheduledupdates", "SPOTFIREOAUTH2\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com"],
    },
    custom_columns=["user_name", "machine", "success"],
    custom_operators={
        "logged_time": ">=",
        "user_name": "!",   # exclude the automation user
    },
)

# Keep only active users
df_wp_logins = df_wp_logins[df_wp_logins["user_name"].isin(ACTIVE_USERNAMES)].copy()
df_wp_logins["platform"] = "Web Player"


# 8b. Desktop/Cloud logins (auth_pro, success = 1)
df_pro_logins = getData(
    params={
        "data_type": "spotfire_if2sf_actionlog",
        "MLR": "T",
        "log_category": "auth_pro",
        "log_action": "login",
        "logged_time": cutoff_str,
        "success": "1",
        "user_name": ["SPOTFIRESYSTEM\\automationservices", "SPOTFIRESYSTEM\monitoring", "SPOTFIRESYSTEM\scheduledupdates", "SPOTFIREOAUTH2\a72082b286310fe3c8d48129c26b295f.oauth-clients.spotfire.tibco.com"],
    },
    custom_columns=["user_name", "machine", "success"],
    custom_operators={
        "logged_time": ">=",
        "user_name": "!",   # exclude automation user
    },
)

df_pro_logins = df_pro_logins[df_pro_logins["user_name"].isin(ACTIVE_USERNAMES)].copy()

# Classify auth_pro logins based on machine IP
def classify_pro_platform(ip: str) -> str:
    if ip in CLOUD_IPS:
        return "Cloud"
    if ip in LOCAL_IPS:
        return "Local Desktop"
    return "Other"

df_pro_logins["platform"] = df_pro_logins["machine"].astype(str).apply(classify_pro_platform)

# 8c. Combine all login records
df_logins_all = pd.concat([df_wp_logins, df_pro_logins], ignore_index=True)

# 8d. Count login events per user & platform
user_platform_logins = (
    df_logins_all
    .groupby(["user_name", "platform"])
    .size()
    .reset_index(name="LOGIN_COUNT")
)

# Optional: restrict to platforms you care about
# user_platform_logins = user_platform_logins[user_platform_logins["platform"].isin(
#     ["Web Player", "Cloud", "Local Desktop"]
# )]

# 8e. Join email / HR info if you want it in this CSV
user_platform_logins = user_platform_logins.merge(
    users[
        [
            "user_name",
            "email",
            "cost_center_name",
            "dept_name",
            "title",
            "TITLE_CATEGORY",
        ]
    ],
    on="user_name",
    how="left",
)

platform_usage_df = user_platform_logins.rename(
    columns={
        "user_name": "USER_NAME",
        "email": "USER_EMAIL",
    }
)

# Now you have columns:
#   USER_NAME, USER_EMAIL, platform, LOGIN_COUNT, cost_center_name, dept_name, title, TITLE_CATEGORY


# ------------------------------------------------------------
# 8. EXPORT ALL CSVs TO S3
# ------------------------------------------------------------
bucket = "spotfire-admin"

# User-level CSV
filename_users = "analyst-functions-users.csv"
s3_path_users = f"s3://{bucket}/{filename_users}"

if s3.chk_file_exist(bucket, filename_users):
    s3.delete_file(bucket=bucket, key=filename_users)

s3.upload_df_as_csv(bucket=bucket, dataframe=final_df, s3_path=s3_path_users)

# Top analyst actions CSV
filename_actions = "analyst-functions-top-actions.csv"
s3_path_actions = f"s3://{bucket}/{filename_actions}"

if s3.chk_file_exist(bucket, filename_actions):
    s3.delete_file(bucket=bucket, key=filename_actions)

s3.upload_df_as_csv(bucket=bucket, dataframe=top_actions_df, s3_path=s3_path_actions)

# Top report views CSV
filename_reports = "top-viewed-reports.csv"
s3_path_reports = f"s3://{bucket}/{filename_reports}"

if s3.chk_file_exist(bucket, filename_reports):
    s3.delete_file(bucket=bucket, key=filename_reports)

s3.upload_df_as_csv(bucket=bucket, dataframe=df_report, s3_path=s3_path_reports)

# Platform usage CSV
filename_platform = "spotfire-platform-logins.csv"
s3_path_platform = f"s3://{bucket}/{filename_platform}"

if s3.chk_file_exist(bucket, filename_platform):
    s3.delete_file(bucket=bucket, key=filename_platform)

s3.upload_df_as_csv(bucket=bucket, dataframe=platform_usage_df, s3_path=s3_path_platform)

print("User-level rows:", len(final_df))
print("Top actions rows:", len(top_actions_df))
print("Top report rows:", len(df_report))
print("Platform usage rows:", len(platform_usage_df))
print("Export complete.")
