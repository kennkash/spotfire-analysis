def _collapse_keep_latest_with_counts(
    df: pd.DataFrame,
    key_col: str,
    time_col: str,
    count_col: str = "view_count",
    extra_count_cols: Optional[Dict[str, str]] = None,  # e.g. {"session_id": "unique_sessions"}
) -> pd.DataFrame:
    """
    Collapse rows by key_col:
      - keep the row with the max time_col as the representative
      - add count_col = total rows in the group
      - optionally add extra aggregate counts (e.g., nunique session_id)

    Returns: representative rows with aggregates attached.
    """
    if df is None or df.empty:
        return df

    d = df.copy()

    # Drop rows with missing key
    d = d.dropna(subset=[key_col])
    if d.empty:
        return d

    # Ensure datetime
    d[time_col] = pd.to_datetime(d[time_col], errors="coerce", utc=True)
    d = d.dropna(subset=[time_col])
    if d.empty:
        return d

    # Aggregations
    grp = d.groupby(key_col, dropna=False)
    counts = grp.size().rename(count_col)

    # Representative row index = idx of latest timestamp per key
    idx = grp[time_col].idxmax()
    rep = d.loc[idx].copy()

    rep[count_col] = rep[key_col].map(counts)

    if extra_count_cols:
        for col, out_name in extra_count_cols.items():
            if col in d.columns:
                rep[out_name] = rep[key_col].map(grp[col].nunique().rename(out_name))

    # Helpful explicit "last_logged" field (same as rep[time_col])
    rep["last_logged"] = rep[time_col]

    # Sort newest first
    rep = rep.sort_values(time_col, ascending=False).reset_index(drop=True)
    return rep

# Identity key: prefer email, fallback user_name
df_reports["_identity_key"] = df_reports["email"].fillna(df_reports["user_name"])

# Collapse to ONE row per identity, but KEEP total views as view_count
# (and optionally unique sessions)
df_reports = _collapse_keep_latest_with_counts(
    df=df_reports,
    key_col="_identity_key",
    time_col="logged_time",
    count_col="view_count",
    extra_count_cols={"session_id": "unique_sessions"} if "session_id" in df_reports.columns else None,
)

# Cleanup key column if you don't want to expose it
df_reports.drop(columns=["_identity_key"], inplace=True, errors="ignore")




# --- Dedupe by FULL_NAME (keep latest logged_time), but SUM view_count ---
if "FULL_NAME" in df_reports.columns:
    # Don't collapse unresolved users into one mega-row
    good_name = df_reports["FULL_NAME"].notna() & (df_reports["FULL_NAME"] != "Possibly Terminated")

    df_good = df_reports.loc[good_name].copy()
    df_bad = df_reports.loc[~good_name].copy()

    if not df_good.empty:
        # If view_count isn't present for some reason, default it
        if "view_count" not in df_good.columns:
            df_good["view_count"] = 1

        # Collapse by FULL_NAME: keep latest row, sum view_count (and unique_sessions if present)
        key = "FULL_NAME"
        grp = df_good.groupby(key, dropna=False)

        # representative rows (latest)
        idx = grp["logged_time"].idxmax()
        rep = df_good.loc[idx].copy()

        # sum counts
        rep["view_count"] = rep[key].map(grp["view_count"].sum())

        if "unique_sessions" in df_good.columns:
            # Note: summing nunique-per-identity can overcount if sessions overlap across identities.
            # If you truly need exact unique sessions per FULL_NAME, we can compute it with a set-based agg.
            rep["unique_sessions"] = rep[key].map(grp["unique_sessions"].sum())

        rep["last_logged"] = rep["logged_time"]
        df_reports = pd.concat([rep, df_bad], ignore_index=True).sort_values("logged_time", ascending=False).reset_index(drop=True)
    else:
        df_reports = df_bad.sort_values("logged_time", ascending=False).reset_index(drop=True)
