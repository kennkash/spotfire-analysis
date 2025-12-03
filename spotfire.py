# ------------------------------------------------------------------
# 0. IMPORTS & SET‑UP
# ------------------------------------------------------------------
import pandas as pd
from datetime import datetime, timedelta
import pytz
from bigdataloader2 import getData  # your external loader
import numpy as np
import s2cloudapi.s3api as s3
import pickle


TZ_CDT = pytz.timezone("America/Chicago")
TZ_UTC = pytz.UTC
# 90 days back from *now* (use UTC – the ETL usually stores UTC timestamps)
cutoff_dt = datetime.utcnow() - timedelta(days=90)
cutoff_dt = cutoff_dt.replace(hour=0, minute=0, second=0, microsecond=0)
cutoff_str = cutoff_dt.strftime(
    "%d-%b-%y %I.%M.%S.%f %p"
)  # e.g. 26-MAY-23 12.05.15.637059 PM

print(cutoff_str)



def utc_to_cdt(series: pd.Series) -> pd.Series:
    """
    Convert a UTC‑aware datetime Series to Central Daylight Time (CDT) and
    format it exactly like the Oracle `TO_CHAR(...,'mm/dd/yyyy hh24:mi:ss')`.
    """
    # Make sure the Series is timezone‑aware UTC
    if series.dt.tz is None:
        series = series.dt.tz_localize(TZ_UTC)
    else:
        series = series.dt.tz_convert(TZ_UTC)

    # Convert to America/Chicago (CDT or CST, depending on the date)
    cdt = series.dt.tz_convert(TZ_CDT)

    # Return the string representation required by the report
    return cdt.dt.strftime("%m/%d/%Y %H:%M:%S")


def analyst_percent(analyst: pd.Series, non_analyst: pd.Series) -> pd.Series:
    """
    Implements the CASE / ROUND logic from the SQL query.
    Returns a float (0‑100) rounded to two decimal places.
    """
    total = analyst + non_analyst
    # Avoid division‑by‑zero
    pct = np.where(total == 0, 0,
                   np.round((analyst / total) * 100.0, 2))
    return pd.Series(pct, index=analyst.index)






# ------------------------------------------------------------------
# 1. DEFINITION OF USER & CATEGORY SETTINGS
# ------------------------------------------------------------------
# Users we never want to report on
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
]

# Categories that *do* belong to an “analyst” workflow
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

# Categories that must be stripped from **both** groups
EXCLUDE_CATEGORIES = [
    "admin",
    "analysis_as",
    "auth",
    "auth_as",
    "auth_pro",
    "auth_wp",
    "automation_job_as",
    "automation_task_as",
    "codetrust",
    "dblogging",
    "ems",
    "library",
    "library_as",
    "monitoring",
    "monitoring_wp",
    "routing_rules",
    "scheduled_updates",
]

# ------------------------------------------------------------------
# 2. LOADING THE FOUR RAW TABLES
# ------------------------------------------------------------------
# 2a. Users
user_columns = [
    "email",
    "last_login",
    "last_modified_membership",
    "user_id",
    "user_name",
]

# 1️⃣  Users that are recent by last_login
params_login = {
    "data_type": "spotfire_if2sf_users",
    "MLR": "T",
    "last_login": cutoff_str,
    "user_name": USERNAME_EXCLUDES,
}

custom_operators = {"last_login": ">=", "user_name": "!"}

users_df = getData(
    params=params_login, custom_columns=user_columns, custom_operators=custom_operators
)

# The ETL already removed USERNAME_EXCLUDES and applied the 90‑day cutoff.
# Just make sure the timestamp column is a proper datetime (UTC).
users_df["last_login"] = pd.to_datetime(
    users_df["last_login"],
    format="%d-%b-%y %I.%M.%S.%f %p",   # same format you used for `cutoff_str`
    utc=True
)

users = users_df[["user_id", "user_name", "last_login", "email"]].copy()


params = {'data_type': 'pageradm_employee_ghr',
'MLR': 'L'}
custom_columns = ['cost_center_name', 'dept_name', 'smtp', 'title']
custom_operators = {'smtp': 'notnull'}
user_data = getData(params=params, custom_columns=custom_columns, custom_operators=custom_operators)



# 2b. ActionLog – we pull all categories, then filter in-code
df_actions = getData(
    params={
        "data_type": "spotfire_if2sf_actionlog",
        "MLR": "T",
        "log_category": EXCLUDE_CATEGORIES,
        "logged_time": cutoff_str,
        "user_name": "SPOTFIRESYSTEM\automationservices"
    },
    custom_columns=["log_action", "log_category", "user_name"],
    custom_operators={"log_category": "!", "logged_time": ">=", "user_name": "!"},
)

# 2c. Groups
df_groups = getData(
    params={
        "data_type": "spotfire_if2sf_groups",
        "MLR": "T",
        "group_name": ["LSI PowerUser", "LSI User"],
    },
    custom_columns=["group_id", "group_name"],
)

group_ids = df_groups["group_id"].unique().tolist()

# 2d. Group members
df_group_members = getData(
    params={
        "data_type": "spotfire_if2sf_group_members",
        "MLR": "T",
        "group_id": group_ids,
    },
    custom_columns=["group_id", "member_user_id"],
)


# print(users_df)
# print(df_actions)
# print(df_groups)
# print(df_group_members)



# Flag every row as “analyst” when its category belongs to ANALYST_CATEGORIES
df_actions["is_analyst"] = df_actions["log_category"].isin(ANALYST_CATEGORIES)

# Aggregate counts per user
action_counts = (
    df_actions
    .groupby("user_name")["is_analyst"]
    .agg(
        analyst_cnt=lambda s: s.sum(),                # True → 1
        non_analyst_cnt=lambda s: (~s).sum()          # False → 1
    )
    .reset_index()
)

users = (
    users
    .merge(action_counts, on="user_name", how="left")
    .fillna({"analyst_cnt": 0, "non_analyst_cnt": 0})
)


# Join members → groups, then keep only the columns we need
user_groups = (
    df_group_members
    .merge(df_groups, on="group_id", how="inner")
    .rename(columns={"member_user_id": "user_id"})   # align with users_df column name
    [["user_id", "group_name"]]
)

# Inner join guarantees that users without one of the two groups are dropped,
# exactly like the original SQL.
users = users.merge(user_groups, on="user_id", how="inner")


# 1️⃣ Convert the UTC login timestamp to CDT and format it
users["LAST_ACTIVITY"] = utc_to_cdt(users["last_login"])

# 2️⃣ Compute the percentage column
users["Percent of Analyst Functions"] = analyst_percent(
    users["analyst_cnt"], users["non_analyst_cnt"]
)

users = users.merge(user_data, left_on="email", right_on="smtp", how="left")

# 3️⃣ Rename columns to match the SQL output (optional but handy)
final_df = users.rename(columns={
    "user_name": "USER_NAME",
    "email":     "USER_EMAIL",
    "group_name": "GROUP_NAME",
    "analyst_cnt": "Analyst functions",
    "non_analyst_cnt": "Non‑Analyst functions"
})[[
    "USER_NAME",
    "GROUP_NAME",
    "LAST_ACTIVITY",
    "Analyst functions",
    "Non‑Analyst functions",
    "Percent of Analyst Functions",
    "cost_center_name",
    "dept_name",
    "title"
]]


# We already have the original UTC datetime (`last_login`) – use it for sorting.
final_df = final_df.sort_values(
    by="LAST_ACTIVITY",               # string order works because format is YYYY‑MM‑DD HH:MM:SS
    ascending=False
).reset_index(drop=True)


print("=== Analyst vs. Non‑Analyst Report ===")
print(final_df.head(10))


# s3.upload_object('spotfire-admin', 'analyst-functions', pickle.dumps(final_df))
bucket = "spotfire-admin"
filename = "analyst-functions.csv"
s3_path = f's3://{bucket}/{filename}' # share path to file

# Cannot overwrite file in bucket
if s3.chk_file_exist(bucket, filename):
    print('Deleting ' + filename)
    s3.delete_file(bucket=bucket, key=filename)

# upload df to shared bucket as csv
s3.upload_df_as_csv(bucket=bucket, dataframe=final_df, s3_path=s3_path)
