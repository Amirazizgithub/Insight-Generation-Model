from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from app.utils.logger import log
import numpy as np
import pandas as pd
import warnings

warnings.filterwarnings("ignore")


# Data classes
@dataclass
class AnalysisResult:
    window_growth: pd.DataFrame = field(default_factory=pd.DataFrame)
    top_n_by_dimension: Dict[str, Dict[int, pd.DataFrame]] = field(default_factory=dict)
    data_quality: Dict[str, dict] = field(default_factory=dict)
    report: str = ""

    def to_dict(self) -> dict:
        """Serialize all fields to JSON-compatible types."""
        return {
            "window_growth": self._df_to_json(self.window_growth),
            "top_n_by_dimension": {
                dim: {
                    window: self._df_to_json(df) for window, df in window_dict.items()
                }
                for dim, window_dict in self.top_n_by_dimension.items()
            },
            "data_quality": self.data_quality,  # already a plain dict
            "report": self.report,
        }

    def to_json(self, **kwargs) -> str:
        """Return a JSON string. Pass any json.dumps kwargs (e.g. indent=2)."""
        import json

        return json.dumps(self.to_dict(), **kwargs)

    @staticmethod
    def _df_to_json(df: pd.DataFrame) -> list[dict]:
        """Convert a DataFrame to a list of row-dicts, handling Timestamps."""
        if df is None or df.empty:
            return []
        # Convert Timestamp columns to ISO strings so they survive JSON serialisation
        df_copy = df.copy()
        for col in df_copy.select_dtypes(include=["datetime64[ns]", "datetimetz"]):
            df_copy[col] = df_copy[col].dt.strftime("%Y-%m-%d")
        return df_copy.to_dict(orient="records")


def preprocess(
    df: pd.DataFrame,
    date_col: str,
    dimension_col: str,
    metrics: List[str],
    agg: str = "sum",
    min_periods: int = 3,
) -> pd.DataFrame:

    log.info("Preprocessing: %d rows × %d cols", len(df), len(df.columns))
    df = df.copy()

    # ── date normalisation ──────────────────────────────────────────────────
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=False)
    if hasattr(df[date_col].dtype, "tz") and df[date_col].dtype.tz is not None:
        df[date_col] = df[date_col].dt.tz_convert(None)
    df[date_col] = df[date_col].dt.normalize()
    df = df.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)

    # ── metric coercion ─────────────────────────────────────────────────────
    for m in metrics:
        if m not in df.columns:
            raise ValueError(f"Metric '{m}' not found in DataFrame columns.")
        df[m] = pd.to_numeric(df[m], errors="coerce")

    # ── dimension coercion ──────────────────────────────────────────────────
    df[dimension_col] = df[dimension_col].astype(str).str.strip()

    # ── daily aggregation ───────────────────────────────────────────────────
    agg_funcs = {m: agg for m in metrics}
    daily = (
        df.groupby([date_col, dimension_col], sort=True).agg(agg_funcs).reset_index()
    )

    log.info(
        "After aggregation: %d rows | date range %s → %s",
        len(daily),
        daily[date_col].min().date(),
        daily[date_col].max().date(),
    )
    return daily


def _growth_flag(cur: float, prev: float, insufficient: bool) -> str:
    if insufficient:
        return "insufficient_data"
    if cur == prev:
        return "no_change"
    return "increase" if cur > prev else "decrease"


def compute_window_growth(
    daily: pd.DataFrame,
    date_col: str,
    metrics: List[str],
    windows: List[int],
    end_date: Optional[pd.Timestamp] = None,
    min_periods: int = 3,
) -> pd.DataFrame:

    end_date = pd.Timestamp(end_date) if end_date else daily[date_col].max()
    log.info("Window growth end_date=%s, windows=%s", end_date.date(), windows)

    # Collapse across dimension for overall totals
    totals = daily.groupby(date_col)[metrics].sum()

    rows = []
    for w in windows:
        cur_start = end_date - pd.Timedelta(days=w - 1)
        prev_end = cur_start - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=w - 1)

        cur_data = totals.loc[cur_start:end_date]
        prev_data = totals.loc[prev_start:prev_end]

        for m in metrics:
            cur_obs = cur_data[m].dropna()
            prev_obs = prev_data[m].dropna()
            insufficient = (len(cur_obs) < min_periods) or (len(prev_obs) < min_periods)

            cur_sum = float(cur_obs.sum()) if len(cur_obs) else 0.0
            prev_sum = float(prev_obs.sum()) if len(prev_obs) else 0.0
            abs_chg = cur_sum - prev_sum

            if insufficient or prev_sum == 0:
                pct_chg = float("inf") if (not insufficient and cur_sum != 0) else None
            else:
                pct_chg = abs_chg / prev_sum * 100

            rows.append(
                {
                    "metric": m,
                    "window_days": w,
                    "current_sum": round(cur_sum, 4),
                    "previous_sum": round(prev_sum, 4),
                    "absolute_change": round(abs_chg, 4),
                    "percent_change": (
                        round(pct_chg, 2)
                        if pct_chg is not None and np.isfinite(pct_chg)
                        else pct_chg
                    ),
                    "growth_flag": _growth_flag(cur_sum, prev_sum, insufficient),
                }
            )

    result = pd.DataFrame(rows)
    log.info("Window growth computed: %d rows", len(result))
    return result


# Top dimension by window group


def top_n_by_dimension(
    daily: pd.DataFrame,
    date_col: str,
    dimension_col: str,
    metrics: List[str],
    windows: List[int],
    top_n: int = 5,
    end_date: Optional[pd.Timestamp] = None,
    include_others: bool = True,
) -> Dict[str, Dict[int, pd.DataFrame]]:
    end_date = pd.Timestamp(end_date) if end_date else daily[date_col].max()
    results: Dict[str, Dict[int, pd.DataFrame]] = {m: {} for m in metrics}

    # Compute once outside loops — dimension count doesn't change per window/metric
    effective_top_n = min(top_n, daily[dimension_col].nunique())

    for w in windows:
        cur_start = end_date - pd.Timedelta(days=w - 1)
        prev_end = cur_start - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=w - 1)

        cur_data = (
            daily[daily[date_col].between(cur_start, end_date)]
            .groupby(dimension_col)[metrics]
            .sum()
            .rename(columns={m: f"{m}_cur" for m in metrics})
        )
        prev_data = (
            daily[daily[date_col].between(prev_start, prev_end)]
            .groupby(dimension_col)[metrics]
            .sum()
            .rename(columns={m: f"{m}_prev" for m in metrics})
        )
        combined = cur_data.join(prev_data, how="outer").fillna(0)

        for m in metrics:
            cur_col, prev_col = f"{m}_cur", f"{m}_prev"
            sub = combined[[cur_col, prev_col]].copy()
            sub.columns = ["current_sum", "previous_sum"]
            sub["absolute_change"] = sub["current_sum"] - sub["previous_sum"]
            sub["percent_change"] = np.where(
                sub["previous_sum"] == 0,
                np.inf,
                (sub["current_sum"] - sub["previous_sum"]) / sub["previous_sum"] * 100,
            ).round(2)

            top = sub.nlargest(effective_top_n, "current_sum").copy()
            top["rank"] = range(1, len(top) + 1)
            top.index.name = dimension_col
            top = top.reset_index()

            bottom = sub.nsmallest(effective_top_n, "current_sum").copy()
            # ✅ Use effective_top_n instead of hardcoded 5
            bottom["rank"] = range(len(bottom) + effective_top_n, len(bottom), -1)
            bottom.index.name = dimension_col
            bottom = bottom.reset_index()

            results[m][w] = pd.concat([top, bottom], ignore_index=True)

    return results


def detect_data_quality(
    raw_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    date_col: str,
    dimension_col: str,
    metrics: List[str],
    iqr_multiplier: float = 1.5,
    zscore_threshold: float = 3.0,
    isoforest_contamination: float = 0.05,
) -> Dict[str, object]:

    log.info(
        "detect_data_quality: raw=%d rows, daily=%d rows", len(raw_df), len(daily_df)
    )

    result: Dict[str, object] = {}
    health_score = 100.0
    health_flags: List[str] = []

    # ── 1. Null / missing value analysis ─────────────────────────────────────
    audit_cols = metrics + [dimension_col, date_col]
    null_rows = []
    for col in audit_cols:
        if col not in raw_df.columns:
            continue
        n_null = int(raw_df[col].isna().sum())
        pct = n_null / len(raw_df) * 100 if len(raw_df) else 0.0
        severity = (
            "none"
            if pct == 0
            else (
                "low"
                if pct < 5
                else "medium" if pct < 20 else "high" if pct < 50 else "critical"
            )
        )
        null_rows.append(
            {
                "column": col,
                "total_rows": len(raw_df),
                "null_count": n_null,
                "null_pct": round(pct, 2),
                "severity": severity,
            }
        )
        if pct > 0:
            penalty = min(pct * 0.5, 20)
            health_score -= penalty
            if pct >= 20:
                health_flags.append(f"HIGH nulls in '{col}': {pct:.1f}% missing")
            elif pct >= 5:
                health_flags.append(f"Moderate nulls in '{col}': {pct:.1f}% missing")

    result["null_summary"] = pd.DataFrame(null_rows)

    # ── null breakdown by dimension value ─────────────────────────────────────
    if dimension_col in raw_df.columns:
        null_by_dim = (
            raw_df.groupby(dimension_col)[metrics]
            .apply(lambda g: g.isna().sum())
            .reset_index()
        )
        result["null_by_dimension"] = null_by_dim
    else:
        result["null_by_dimension"] = pd.DataFrame()

    # ── 2. Duplicate rows ────────────────────────────────────────────────────
    dupes = raw_df[raw_df.duplicated(keep=False)].copy()
    result["duplicate_rows"] = dupes
    if len(dupes):
        dupe_pct = len(dupes) / len(raw_df) * 100
        health_score -= min(dupe_pct * 0.3, 10)
        health_flags.append(
            f"Duplicate rows detected: {len(dupes)} rows ({dupe_pct:.1f}%)"
        )

    # ── 3. Negative metric values ────────────────────────────────────────────
    neg_rows = []
    for m in metrics:
        if m not in raw_df.columns:
            continue
        mask = raw_df[m].notna() & (raw_df[m] < 0)
        bad = raw_df[mask].copy()
        if len(bad):
            bad["_metric"] = m
            bad["_bad_value"] = bad[m]
            neg_rows.append(bad)
            health_score -= min(len(bad) * 0.2, 5)
            health_flags.append(f"Negative values in '{m}': {len(bad)} rows")

    result["negative_values"] = (
        pd.concat(neg_rows, ignore_index=True) if neg_rows else pd.DataFrame()
    )

    # ── 4. Date gaps per dimension value ─────────────────────────────────────
    gap_rows = []
    if date_col in daily_df.columns and dimension_col in daily_df.columns:
        full_range = pd.date_range(
            daily_df[date_col].min(), daily_df[date_col].max(), freq="D"
        )
        for dim_val, grp in daily_df.groupby(dimension_col):
            present_dates = set(grp[date_col].dt.normalize())
            missing = sorted(
                full_range.difference(pd.DatetimeIndex(list(present_dates)))
            )
            if not missing:
                continue
            # Collapse consecutive missing dates into ranges
            if missing:
                segments: List[Tuple] = []
                seg_start = missing[0]
                seg_end = missing[0]
                for d in missing[1:]:
                    if (d - seg_end).days == 1:
                        seg_end = d
                    else:
                        segments.append((seg_start, seg_end))
                        seg_start = seg_end = d
                segments.append((seg_start, seg_end))
                for gs, ge in segments:
                    gap_days = (ge - gs).days + 1
                    gap_rows.append(
                        {
                            "dimension_value": dim_val,
                            "gap_start": str(gs.date()),
                            "gap_end": str(ge.date()),
                            "gap_days": gap_days,
                        }
                    )
        if gap_rows:
            total_gap_days = sum(r["gap_days"] for r in gap_rows)
            health_score -= min(total_gap_days * 0.05, 10)
            health_flags.append(
                f"Date gaps found: {len(gap_rows)} gap segment(s) "
                f"across {daily_df[dimension_col].nunique()} dimension values"
            )

    result["date_gaps"] = pd.DataFrame(gap_rows)

    # ── 5. Outlier detection (IQR + Z-score + Isolation Forest) ──────────────
    totals = daily_df.groupby(date_col)[metrics].sum().reset_index()

    outlier_summary_rows = []
    all_outlier_frames: List[pd.DataFrame] = []

    for m in metrics:
        col = totals[m].dropna()
        if len(col) < 4:
            continue

        Q1, Q3 = col.quantile(0.25), col.quantile(0.75)
        IQR = Q3 - Q1
        lower_iqr = Q1 - iqr_multiplier * IQR
        upper_iqr = Q3 + iqr_multiplier * IQR

        mean_val, std_val = col.mean(), col.std()
        lower_z = mean_val - zscore_threshold * std_val
        upper_z = mean_val + zscore_threshold * std_val

        # Keep ALL rows in working — do NOT dropna() here.
        # dropna() silently removes entire metrics that have any NaN dates in
        # the totals pivot, causing them to vanish from outlier_rows_df.
        # Instead, compute flags only where values are non-null (valid_mask).
        working = totals[[date_col, m]].copy()
        valid_mask = working[m].notna()
        working["_zscore"] = np.where(
            valid_mask,
            (working[m] - mean_val) / std_val if std_val > 0 else 0.0,
            np.nan,
        )
        working["is_iqr_outlier"] = valid_mask & (
            (working[m] < lower_iqr) | (working[m] > upper_iqr)
        )
        working["is_zscore_outlier"] = valid_mask & (
            working["_zscore"].abs() > zscore_threshold
        )
        working["is_isoforest_outlier"] = False

        # Isolation Forest (optional — skip gracefully if sklearn missing)
        if isoforest_contamination > 0 and len(col) >= 20:
            try:
                from sklearn.ensemble import IsolationForest

                iso = IsolationForest(
                    contamination=isoforest_contamination,
                    random_state=42,
                    n_estimators=100,
                )
                preds = iso.fit_predict(working[[m]])
                working["is_isoforest_outlier"] = preds == -1
            except ImportError:
                log.debug("sklearn not installed — Isolation Forest skipped for %s", m)
            except Exception as e:
                log.debug("Isolation Forest failed for %s: %s", m, e)

        n_iqr = int(working["is_iqr_outlier"].sum())
        n_z = int(working["is_zscore_outlier"].sum())
        n_iso = int(working["is_isoforest_outlier"].sum())

        outlier_summary_rows.append(
            {
                "metric": m,
                "n_observations": len(col),
                "mean": round(mean_val, 4),
                "std": round(std_val, 4),
                "Q1": round(Q1, 4),
                "Q3": round(Q3, 4),
                "IQR": round(IQR, 4),
                "lower_iqr_fence": round(lower_iqr, 4),
                "upper_iqr_fence": round(upper_iqr, 4),
                "lower_zscore_fence": round(lower_z, 4),
                "upper_zscore_fence": round(upper_z, 4),
                "n_iqr_outliers": n_iqr,
                "n_zscore_outliers": n_z,
                "n_isoforest_outliers": n_iso,
                "n_any_outliers": int(
                    (
                        working["is_iqr_outlier"]
                        | working["is_zscore_outlier"]
                        | working["is_isoforest_outlier"]
                    ).sum()
                ),
            }
        )

        flagged = working[
            working["is_iqr_outlier"]
            | working["is_zscore_outlier"]
            | working["is_isoforest_outlier"]
        ].copy()
        if len(flagged):
            # Normalise to fixed schema: [date_col, metric, value, _zscore,
            # is_iqr_outlier, is_zscore_outlier, is_isoforest_outlier, outlier_methods]
            # Renaming the metric column to "value" means every frame has
            # identical columns so pd.concat never creates sparse/misaligned
            # frames that make certain metrics disappear from outlier_rows_df.
            flagged = flagged.rename(columns={m: "value"})
            flagged["metric"] = m
            flagged["outlier_methods"] = flagged.apply(
                lambda r: ", ".join(
                    filter(
                        None,
                        [
                            "IQR" if r["is_iqr_outlier"] else "",
                            "Z-score" if r["is_zscore_outlier"] else "",
                            "IsolationForest" if r["is_isoforest_outlier"] else "",
                        ],
                    )
                ),
                axis=1,
            )
            # Keep only the columns needed for reporting — drop the raw metric
            # columns from other iterations that may have leaked into working.
            keep_cols = [
                date_col,
                "metric",
                "value",
                "_zscore",
                "is_iqr_outlier",
                "is_zscore_outlier",
                "is_isoforest_outlier",
                "outlier_methods",
            ]
            flagged = flagged[[c for c in keep_cols if c in flagged.columns]]
            all_outlier_frames.append(flagged)

    result["outlier_summary"] = pd.DataFrame(outlier_summary_rows)

    outlier_rows_df = (
        pd.concat(all_outlier_frames, ignore_index=True)
        if all_outlier_frames
        else pd.DataFrame()
    )
    result["outlier_rows"] = outlier_rows_df

    if not outlier_rows_df.empty and not result["outlier_summary"].empty:
        total_obs = result["outlier_summary"]["n_observations"].sum()
        total_out = result["outlier_summary"]["n_any_outliers"].sum()
        out_pct = total_out / total_obs * 100 if total_obs else 0
        if out_pct > 10:
            health_score -= min(out_pct * 0.4, 15)
            health_flags.append(
                f"High outlier rate across metrics: {total_out} flagged observations ({out_pct:.1f}%)"
            )
        elif out_pct > 3:
            health_score -= min(out_pct * 0.2, 8)
            health_flags.append(
                f"Moderate outlier rate: {total_out} flagged observations ({out_pct:.1f}%)"
            )

    # ── 6. Spike / drop detection (day-over-day Z-score on rolling window) ───
    spike_rows = []
    daily_totals = daily_df.groupby(date_col)[metrics].sum().sort_index()

    for m in metrics:
        series = daily_totals[m].dropna()
        if len(series) < 7:
            continue
        dod = series.diff()
        rolling_std = dod.rolling(window=7, min_periods=3).std()
        z_dod = dod / rolling_std.replace(0, np.nan)

        spikes = z_dod[z_dod.abs() > 3].dropna()
        for dt, z in spikes.items():
            idx = series.index.get_loc(dt)
            prev_val = float(series.iloc[idx - 1]) if idx > 0 else float("nan")
            spike_rows.append(
                {
                    "date": str(dt.date()) if hasattr(dt, "date") else str(dt),
                    "metric": m,
                    "value": round(float(series[dt]), 4),
                    "prev_value": round(prev_val, 4),
                    "day_change": round(float(dod[dt]), 4),
                    "rolling_std": round(float(rolling_std[dt]), 4),
                    "zscore_change": round(float(z), 4),
                    "direction": "spike" if float(dod[dt]) > 0 else "drop",
                }
            )

    spike_df = pd.DataFrame(spike_rows)
    result["spike_drop_events"] = spike_df
    if len(spike_df):
        health_score -= min(len(spike_df) * 1.0, 10)
        health_flags.append(
            f"Sudden spike/drop events detected: {len(spike_df)} day(s) with anomalous day-over-day change"
        )

    # ── 7. Flat-line segments (metric constant for 3+ consecutive days) ───────
    # ── 7. Flat-line segments (metric constant for 3+ consecutive days) ───────
    flat_rows = []
    for m in metrics:
        series = daily_totals[m].dropna()
        if len(series) < 3:
            continue
        # Compare each value to the previous; fillna(False) turns the first-row
        # NA (from shift) into False so the boolean check is always safe.
        same = series.eq(series.shift()).fillna(False)
        run_start = None
        run_val = None
        run_idx = 0
        for i, (dt, is_same) in enumerate(same.items()):
            if not bool(is_same):  # bool() is safe now — no NAs remain
                if run_start is not None and i - run_idx >= 3:
                    flat_rows.append(
                        {
                            "metric": m,
                            "start_date": (
                                str(run_start.date())
                                if hasattr(run_start, "date")
                                else str(run_start)
                            ),
                            "end_date": (
                                str(series.index[i - 1].date())
                                if hasattr(series.index[i - 1], "date")
                                else str(series.index[i - 1])
                            ),
                            "duration_days": i - run_idx,
                            "constant_value": run_val,
                        }
                    )
                run_start = dt
                run_val = float(series.iloc[i])
                run_idx = i
        # Handle an open flat-line run that reaches the end of the series
        if run_start is not None and len(series) - run_idx >= 3:
            flat_rows.append(
                {
                    "metric": m,
                    "start_date": (
                        str(run_start.date())
                        if hasattr(run_start, "date")
                        else str(run_start)
                    ),
                    "end_date": (
                        str(series.index[-1].date())
                        if hasattr(series.index[-1], "date")
                        else str(series.index[-1])
                    ),
                    "duration_days": len(series) - run_idx,
                    "constant_value": run_val,
                }
            )

    flat_df = pd.DataFrame(flat_rows)
    result["flatline_segments"] = flat_df
    if len(flat_df):
        health_score -= min(len(flat_df) * 2.0, 8)
        health_flags.append(
            f"Flat-line segments detected: {len(flat_df)} metric(s) with 3+ consecutive identical daily values"
        )

    # ── 8. Final health score & flags ─────────────────────────────────────────
    health_score = max(0.0, min(100.0, health_score))
    result["overall_health_score"] = round(health_score, 1)

    if not health_flags:
        health_flags.append("No significant data quality issues detected. ✓")
    result["health_flags"] = health_flags

    log.info(
        "Data quality audit complete | health=%.1f | flags=%d | outliers=%d | spikes=%d",
        health_score,
        len(health_flags),
        len(outlier_rows_df),
        len(spike_df),
    )
    return result


# 7. Report
def _build_report(
    window_growth: pd.DataFrame,
    top_n_results: Dict[str, Dict[int, pd.DataFrame]],
    data_quality: Dict[str, object],
    dimension_col: str,
    metrics: List[str],
    windows: List[int],
) -> str:
    lines = ["=" * 70, "  ECOM ANALYTICS REPORT", "=" * 70, ""]

    # ── 0. Data quality health banner ────────────────────────────────────────
    score = data_quality.get("overall_health_score", 100.0)
    bar_filled = int(score / 5)
    bar = "█" * bar_filled + "░" * (20 - bar_filled)
    grade = (
        "HEALTHY ✓"
        if score >= 90
        else ("MODERATE ⚠" if score >= 70 else "NEEDS ATTENTION ✗")
    )
    lines += [
        "▶ DATA HEALTH OVERVIEW",
        "-" * 40,
        f"  Health Score : {score:.1f}/100  [{bar}]  {grade}",
        "",
    ]
    for flag in data_quality.get("health_flags", []):
        lines.append(
            f"  {'⚠' if 'issue' not in flag.lower() and '✓' not in flag else '✓'}  {flag}"
        )
    lines.append("")

    # ── 1. Null / missing analysis ───────────────────────────────────────────
    null_df: pd.DataFrame = data_quality.get("null_summary", pd.DataFrame())
    lines += ["▶ NULL / MISSING VALUE SUMMARY", "-" * 40]
    if null_df.empty:
        lines.append("  No null analysis available.")
    else:
        problematic = null_df[null_df["null_count"] > 0]
        if problematic.empty:
            lines.append("  All columns fully populated — no nulls detected. ✓")
        else:
            for _, row in problematic.iterrows():
                bar_w = int(row["null_pct"] / 5)
                sev_icon = {
                    "low": "○",
                    "medium": "◑",
                    "high": "●",
                    "critical": "⬤",
                }.get(row["severity"], "○")
                lines.append(
                    f"  {sev_icon} {row['column']:<25}  "
                    f"{row['null_count']:>6} nulls  "
                    f"({row['null_pct']:5.1f}%)  "
                    f"[{'█' * bar_w + '░' * (10 - bar_w)}]  "
                    f"{row['severity'].upper()}"
                )
    lines.append("")

    # ── 2. Duplicates ────────────────────────────────────────────────────────
    dupe_df: pd.DataFrame = data_quality.get("duplicate_rows", pd.DataFrame())
    lines += ["▶ DUPLICATE ROWS", "-" * 40]
    if dupe_df.empty:
        lines.append("  No duplicate rows detected. ✓")
    else:
        lines.append(
            f"  ⚠  {len(dupe_df)} fully-duplicated rows found — review ingestion pipeline."
        )
    lines.append("")

    # ── 3. Negative values ───────────────────────────────────────────────────
    neg_df: pd.DataFrame = data_quality.get("negative_values", pd.DataFrame())
    lines += ["▶ NEGATIVE METRIC VALUES", "-" * 40]
    if neg_df.empty:
        lines.append("  No negative metric values detected. ✓")
    else:
        grp = neg_df.groupby("_metric")["_bad_value"]
        for m_name, vals in grp:
            lines.append(
                f"  ⚠  '{m_name}': {len(vals)} negative row(s)  "
                f"[min={vals.min():.2f}, max={vals.max():.2f}]"
            )
    lines.append("")

    # ── 4. Date gaps ─────────────────────────────────────────────────────────
    gap_df: pd.DataFrame = data_quality.get("date_gaps", pd.DataFrame())
    lines += ["▶ DATE GAPS (missing calendar days per dimension value)", "-" * 40]
    if gap_df.empty:
        lines.append("  No date gaps detected across any dimension value. ✓")
    else:
        for dim_val, grp in gap_df.groupby("dimension_value"):
            total_missing = grp["gap_days"].sum()
            lines.append(
                f"  ⚠  {str(dim_val):<30}  {total_missing} missing day(s)  "
                f"in {len(grp)} gap segment(s)"
            )
            for _, row in grp.head(3).iterrows():
                lines.append(
                    f"       {row['gap_start']} → {row['gap_end']}  ({row['gap_days']}d)"
                )
            if len(grp) > 3:
                lines.append(f"       … and {len(grp) - 3} more gap(s)")
    lines.append("")

    # ── 5. Outlier summary ───────────────────────────────────────────────────
    out_sum: pd.DataFrame = data_quality.get("outlier_summary", pd.DataFrame())
    out_rows: pd.DataFrame = data_quality.get("outlier_rows", pd.DataFrame())
    lines += ["▶ OUTLIER DETECTION  (IQR · Z-score · Isolation Forest)", "-" * 40]
    if out_sum.empty:
        lines.append("  Insufficient data for outlier detection.")
    else:
        for _, row in out_sum.iterrows():
            n_any = int(row["n_any_outliers"])
            rate = n_any / row["n_observations"] * 100 if row["n_observations"] else 0
            flag_icon = "✓" if n_any == 0 else ("○" if rate < 5 else "⚠")
            lines.append(
                f"  {flag_icon} {row['metric']:<20} "
                f"IQR={row['n_iqr_outliers']:>3}  "
                f"Z-score={row['n_zscore_outliers']:>3}  "
                f"IsoForest={row['n_isoforest_outliers']:>3}  "
                f"→ {n_any} total ({rate:.1f}%)"
            )
            lines.append(
                f"      IQR fence  : [{row['lower_iqr_fence']:,.2f} , {row['upper_iqr_fence']:,.2f}]"
            )
            lines.append(
                f"      Z-score fence: [{row['lower_zscore_fence']:,.2f} , {row['upper_zscore_fence']:,.2f}]"
            )
            # Show worst offenders — outlier_rows now has a fixed "value" column
            if (
                not out_rows.empty
                and "metric" in out_rows.columns
                and "value" in out_rows.columns
            ):
                metric_rows = out_rows[out_rows["metric"] == row["metric"]]
                if not metric_rows.empty:
                    date_col_name = metric_rows.columns[0]  # first col is always date
                    worst = metric_rows.nlargest(3, "value")[
                        [date_col_name, "value", "outlier_methods"]
                    ]
                    for _, wr in worst.iterrows():
                        lines.append(
                            f"      → {wr.iloc[0]}  value={wr['value']:,.2f}  [{wr['outlier_methods']}]"
                        )
    lines.append("")

    # ── 6. Spike & drop events ───────────────────────────────────────────────
    spike_df: pd.DataFrame = data_quality.get("spike_drop_events", pd.DataFrame())
    lines += ["▶ SUDDEN SPIKE / DROP EVENTS  (day-over-day Z-score > 3σ)", "-" * 40]
    if spike_df.empty:
        lines.append("  No sudden spikes or drops detected. ✓")
    else:
        for m_name, grp in spike_df.groupby("metric"):
            lines.append(f"  {m_name}:  {len(grp)} event(s)")
            for _, row in grp.head(5).iterrows():
                icon = "▲" if row["direction"] == "spike" else "▼"
                lines.append(
                    f"    {icon} {row['date']}  "
                    f"value={row['value']:>12,.2f}  "
                    f"prev={row['prev_value']:>12,.2f}  "
                    f"Δ={row['day_change']:>+12,.2f}  "
                    f"z={row['zscore_change']:>+6.2f}σ"
                )
            if len(grp) > 5:
                lines.append(f"    … and {len(grp) - 5} more event(s)")
    lines.append("")

    # ── 7. Flat-line segments ────────────────────────────────────────────────
    flat_df: pd.DataFrame = data_quality.get("flatline_segments", pd.DataFrame())
    lines += [
        "▶ FLAT-LINE SEGMENTS  (metric unchanged for 3+ consecutive days)",
        "-" * 40,
    ]
    if flat_df.empty:
        lines.append("  No flat-line segments detected. ✓")
    else:
        for _, row in flat_df.iterrows():
            lines.append(
                f"  ⚠  {row['metric']:<20} "
                f"{row['start_date']} → {row['end_date']}  "
                f"({row['duration_days']}d)  "
                f"value={row['constant_value']:,.2f}"
            )
    lines.append("")

    # ── Growth summary ───────────────────────────────────────────────────────
    lines.append("WINDOWED GROWTH SUMMARY")
    lines.append("-" * 40)
    for m in metrics:
        sub = window_growth[window_growth["metric"] == m].sort_values("window_days")
        lines.append(f"  {m}:")
        for _, row in sub.iterrows():
            pct = row["percent_change"]
            pct_str = (
                f"{pct:+.2f}%"
                if isinstance(pct, float) and np.isfinite(pct)
                else str(pct)
            )
            flag = row["growth_flag"]
            symbol = "▲" if flag == "increase" else ("▼" if flag == "decrease" else "–")
            lines.append(
                f"    {int(row['window_days']):>4}d window: {pct_str:>10}  {symbol}  {flag}"
            )
    lines.append("")

    # ── Top-N ────────────────────────────────────────────────────────────────
    ref_window = min(windows)
    lines.append(
        f"TOP DIMENSION BREAKDOWN  (window = {ref_window}d, col = '{dimension_col}')"
    )
    lines.append("-" * 40)
    for m in metrics:
        df_top = top_n_results.get(m, {}).get(ref_window, pd.DataFrame())
        if df_top.empty:
            continue
        df_top_clean = df_top[df_top[dimension_col] != "__others__"]
        lines.append(f"  {m}:")
        for _, row in df_top_clean.head(5).iterrows():
            pct = row["percent_change"]
            pct_str = (
                f"{pct:+.2f}%" if isinstance(pct, float) and np.isfinite(pct) else "∞"
            )
            lines.append(
                f"    #{int(row['rank'])}  {str(row[dimension_col]):<25} cur={row['current_sum']:>12,.2f}  Δ={pct_str}"
            )
    lines.append("")
    lines.append("=" * 70)
    return "\n".join(lines)


# 8. Orchestrator


def analyze_cohorts(
    df: pd.DataFrame,
    date_col: str,
    dimension_col: str,
    windows: List[int] = None,
    top_n: int = 5,
    min_periods: int = 3,
    end_date: Optional[str] = None,
    agg: str = "sum",
) -> AnalysisResult:

    metrics = df.select_dtypes(include="number").columns.tolist()
    end_ts = pd.Timestamp(end_date) if end_date else None
    log.info("analyze_cohorts START | metrics=%s | windows=%s", metrics, windows)

    # Step 1 – preprocess
    daily = preprocess(
        df, date_col, dimension_col, metrics, agg=agg, min_periods=min_periods
    )

    # Step 2 – window growth
    wg = compute_window_growth(
        daily, date_col, metrics, windows, end_date=end_ts, min_periods=min_periods
    )

    # Step 3 – top-N
    top_n_res = top_n_by_dimension(
        daily, date_col, dimension_col, metrics, windows, top_n=top_n, end_date=end_ts
    )

    # Step 6 – data quality & anomaly detection
    dq = detect_data_quality(df, daily, date_col, dimension_col, metrics)

    # Step 8 – report (now includes anomaly section)
    report = _build_report(wg, top_n_res, dq, dimension_col, metrics, windows)
    log.info("analyze_cohorts COMPLETE")

    return AnalysisResult(
        window_growth=wg,
        top_n_by_dimension=top_n_res,
        data_quality=dq,
        report=report,
    )
