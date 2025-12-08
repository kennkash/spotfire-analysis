# ------------------------------------------------------------
# 8. PLATFORM USAGE: WEB PLAYER vs CLOUD vs LOCAL DESKTOP
# ------------------------------------------------------------

# Active usernames (last 90 days)
ACTIVE_USERNAMES = set(users["user_name"].unique())

CLOUD_IPS = {"192.12.345.123", "192.12.345.456", "192.12.345.789"}
LOCAL_IPS = {"105.987.65.432"}

# 8a. Web Player logins (auth_wp, success = 1)
df_wp_logins = getData(
    params={
        "data_type": "spotfire_if2sf_actionlog",
        "MLR": "T",
        "log_category": "auth_wp",
        "log_action": "login",
        "logged_time": cutoff_str,
        "success": "1",
        "user_name": "SPOTFIRESYSTEM\\automationservices",
    },
    custom_columns=["user_name", "machine", "success"],
    custom_operators={
        "log_category": "=",
        "log_action": "=",
        "logged_time": ">=",
        "success": "=",
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
        "user_name": "SPOTFIRESYSTEM\\automationservices",
    },
    custom_columns=["user_name", "machine", "success"],
    custom_operators={
        "log_category": "=",
        "log_action": "=",
        "logged_time": ">=",
        "success": "=",
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
    return "Unknown Desktop"

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
    users[["user_name", "email"]],
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
#   USER_NAME, USER_EMAIL, platform, LOGIN_COUNT

# Spotfire can do:
# - Bar chart: X = platform, Y = Sum(LOGIN_COUNT)
# - Breakdown by user / cost center if you merge more fields

# 8f. Export platform usage CSV
bucket = "spotfire-admin"
filename_platform = "spotfire-platform-logins.csv"
s3_path_platform = f"s3://{bucket}/{filename_platform}"

if s3.chk_file_exist(bucket, filename_platform):
    s3.delete_file(bucket=bucket, key=filename_platform)

s3.upload_df_as_csv(
    bucket=bucket,
    dataframe=platform_usage_df,
    s3_path=s3_path_platform,
)

print("Platform usage rows:", len(platform_usage_df))