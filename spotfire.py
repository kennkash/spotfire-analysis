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

    if any(k in t for k in engineer_keywords):
        return "Engineer"
    if any(k in t for k in tech_keywords):
        return "Tech"

    return "Other"


# ------------------------------------------------------------
# 1. SETTINGS
# ------------------------------------------------------------
USERNAME_EXCLUDES = [
    "user1", "user2", "user3",
    "user4", "user5", "user6",
    "user7", "user8", "user9",
    "user10", "user11", "user12",
    "user13", "user14",
]

ANALYST_CATEGORIES = {
    "analysis_pro",
    "data_connection_pro",
    "info_link",
    "datafunction_pro",
    "datasource_pro",
    "file_pro",
    "find_pro",
    "library_pro",
}

EXCLUDE_CATEGORIES = [
    "admin", "analysis_as", "auth", "auth_as", "auth_pro", "auth_wp",
    "automation_job_as", "automation_task_as", "codetrust", "dblogging",
    "ems", "library", "library_as", "monitoring", "monitoring_wp",
    "routing_rules", "scheduled_updates",
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
        "log_category": EXCLUDE_CATEGORIES,
        "logged_time": cutoff_str,
        "user_name": "SPOTFIRESYSTEM\\automationservices",
    },
    custom_columns=["log_action", "log_category", "user_name"],
    custom_operators={
        "log_category": "!",
        "logged_time": ">=",
        "user_name": "!",
    },
)

# Flag each row as analyst / non-analyst
df_actions["is_analyst"] = df_actions["log_category"].isin(ANALYST_CATEGORIES)

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
# 4. MERGE HR DATA (COST CENTER, DEPT, TITLE)
# ------------------------------------------------------------
params_hr = {"data_type": "employee_ghr", "MLR": "L"}
user_data = getData(
    params=params_hr,
    custom_columns=["cost_center_name", "dept_name", "smtp", "title"],
    custom_operators={"smtp": "notnull"},
)

users = users.merge(user_data, left_on="email", right_on="smtp", how="left")


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
# 7. EXPORT BOTH CSVs TO S3
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

print("User-level rows:", len(final_df))
print("Top actions rows:", len(top_actions_df))
print("Export complete.")