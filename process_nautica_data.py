"""
process_nautica_data.py

Reads today's scraped data (data/nautica_raw.xlsx) and adds it to the
starting values (data/starting_values.json) to produce an updated
output (data/nautica_processed.json).

Flow:
  1. Parse today's daily report from FusionSolar
  2. Load starting values (monthly + lifetime baselines)
  3. Add today's values to the current month
  4. Recalculate lifetime for current year (sum of all months in that year)
  5. Save updated nautica_processed.json
  6. Update starting_values.json so next run builds on today's totals
"""

import json
import sys
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import calendar

# Force SAST timezone (UTC+2)
SAST = timezone(timedelta(hours=2))

import pandas as pd


# ── Fields that are summed when adding daily data ──────────────────────────
ADDITIVE_FIELDS = [
    "PV Yield (kWh)",
    "Inverter Yield (kWh)",
    "Export (kWh)",
    "Import (kWh)",
    "Consumption (kWh)",
    "Self-consumption (kWh)",
    "CO₂ Avoided (t)",
    "Standard Coal Saved (t)",
    "Revenue (R.)",
    "Charge (kWh)",
    "Discharge (kWh)",
    "Theoretical Yield (kWh)",
    "Loss Due to Export Limitation (kWh)",
    "Loss Due to Export Limitation(R.)",
]

# ── Fields where we take the max ───────────────────────────────────────────
MAX_FIELDS = [
    "Peak Power (kW)",
]

# ── Fields that are recalculated from other fields ─────────────────────────
# Self-consumption Rate = (Self-consumption / PV Yield) * 100


def parse_daily_report(filepath):
    """Parse the daily xlsx download from FusionSolar.
    
    The file has hourly rows:
    Row 0: Title row
    Row 1: Column headers  
    Row 2+: Hourly data rows (e.g. '2026-02-19 00:00:00' to '2026-02-19 08:00:00')
    """
    df = pd.read_excel(filepath, header=None, sheet_name=0)
    headers = df.iloc[1].tolist()
    
    # Sum all data rows to get daily totals
    combined = {}
    row_count = 0
    
    for idx in range(2, len(df)):
        row = df.iloc[idx]
        period = str(row.iloc[0]) if not pd.isna(row.iloc[0]) else ""
        
        for i, h in enumerate(headers):
            if pd.isna(h) or h in ['Statistical Period', 'Total String Capacity (kWp)']:
                continue
            key = str(h).strip()
            val = float(row.iloc[i]) if not pd.isna(row.iloc[i]) else 0.0
            
            if key in ADDITIVE_FIELDS:
                combined[key] = combined.get(key, 0.0) + val
            elif key in MAX_FIELDS:
                combined[key] = max(combined.get(key, 0.0), val)
            else:
                combined[key] = val
        
        row_count += 1
    
    if row_count == 0:
        print("  ⚠️  No data rows found in daily report")
        return None
    
    print(f"  ✅ Parsed {row_count} row(s) from daily report")
    return combined


def parse_hourly_arrays(filepath):
    """Parse hourly rows from the daily xlsx download.
    
    Returns: {
        'current_hour': int (last hour with data),
        'pv': [24 floats],
        'import': [24 floats],
        'export': [24 floats],
        'load': [24 floats]
    }
    """
    df = pd.read_excel(filepath, header=None, sheet_name=0)
    headers = [str(h).strip() if not pd.isna(h) else '' for h in df.iloc[1].tolist()]
    
    # Find column indices
    pv_col = next((i for i, h in enumerate(headers) if h == 'PV Yield (kWh)'), None)
    exp_col = next((i for i, h in enumerate(headers) if h == 'Export (kWh)'), None)
    imp_col = next((i for i, h in enumerate(headers) if h == 'Import (kWh)'), None)
    
    pv_arr = [0.0] * 24
    imp_arr = [0.0] * 24
    exp_arr = [0.0] * 24
    load_arr = [0.0] * 24
    current_hour = 0
    
    for idx in range(2, len(df)):
        row = df.iloc[idx]
        try:
            ts = pd.Timestamp(row.iloc[0])
            hour = ts.hour
        except:
            continue
        
        pv = float(row.iloc[pv_col]) if pv_col is not None and not pd.isna(row.iloc[pv_col]) else 0.0
        exp = float(row.iloc[exp_col]) if exp_col is not None and not pd.isna(row.iloc[exp_col]) else 0.0
        imp = float(row.iloc[imp_col]) if imp_col is not None and not pd.isna(row.iloc[imp_col]) else 0.0
        
        # Load calculation
        if pv <= 0:
            load = imp
        elif exp > 0:
            load = pv - exp + imp
        else:
            load = pv + imp
        
        pv_arr[hour] = round(pv, 2)
        imp_arr[hour] = round(imp, 2)
        exp_arr[hour] = round(exp, 2)
        load_arr[hour] = round(load, 2)
        current_hour = hour
    
    print(f"  ⏰ Latest hour in data: {current_hour:02d}:00 SAST")
    print(f"  📊 Hourly PV range: {min(v for v in pv_arr if v > 0) if any(v > 0 for v in pv_arr) else 0:.1f} - {max(pv_arr):.1f} kW")
    
    return {
        'current_hour': current_hour,
        'pv': pv_arr,
        'import': imp_arr,
        'export': exp_arr,
        'load': load_arr
    }


def add_daily_to_month(monthly_data, daily_data):
    """Add daily values to monthly totals."""
    updated = dict(monthly_data)
    
    for field in ADDITIVE_FIELDS:
        daily_val = daily_data.get(field, 0.0)
        monthly_val = updated.get(field, 0.0)
        updated[field] = round(monthly_val + daily_val, 3)
    
    for field in MAX_FIELDS:
        daily_val = daily_data.get(field, 0.0)
        monthly_val = updated.get(field, 0.0)
        updated[field] = round(max(monthly_val, daily_val), 3)
    
    # Recalculate self-consumption rate
    pv_yield = updated.get("PV Yield (kWh)", 0.0)
    self_consumption = updated.get("Self-consumption (kWh)", 0.0)
    if pv_yield > 0:
        updated["Self-consumption Rate (%)"] = round((self_consumption / pv_yield) * 100, 3)
    
    return updated


def recalculate_lifetime_year(monthly_data, year_str):
    """Recalculate a lifetime year entry by summing all months in that year."""
    year_total = {}
    
    # Find all months for this year
    matching_months = {k: v for k, v in monthly_data.items() if k.startswith(year_str)}
    
    if not matching_months:
        return None
    
    for month_key, month_vals in matching_months.items():
        for field in ADDITIVE_FIELDS:
            year_total[field] = year_total.get(field, 0.0) + month_vals.get(field, 0.0)
        for field in MAX_FIELDS:
            year_total[field] = max(year_total.get(field, 0.0), month_vals.get(field, 0.0))
    
    # Round all values
    for key in year_total:
        year_total[key] = round(year_total[key], 3)
    
    # Recalculate self-consumption rate for the year
    pv_yield = year_total.get("PV Yield (kWh)", 0.0)
    self_consumption = year_total.get("Self-consumption (kWh)", 0.0)
    if pv_yield > 0:
        year_total["Self-consumption Rate (%)"] = round((self_consumption / pv_yield) * 100, 3)
    
    return year_total


def calculate_all_time_totals(lifetime_data):
    """Calculate grand totals across all years."""
    totals = {}
    
    for year_key, year_vals in lifetime_data.items():
        for field in ADDITIVE_FIELDS:
            totals[field] = totals.get(field, 0.0) + year_vals.get(field, 0.0)
        for field in MAX_FIELDS:
            totals[field] = max(totals.get(field, 0.0), year_vals.get(field, 0.0))
    
    # Round
    for key in totals:
        totals[key] = round(totals[key], 3)
    
    # Recalculate rate
    pv_yield = totals.get("PV Yield (kWh)", 0.0)
    self_consumption = totals.get("Self-consumption (kWh)", 0.0)
    if pv_yield > 0:
        totals["Self-consumption Rate (%)"] = round((self_consumption / pv_yield) * 100, 3)
    
    return totals


def main():
    data_dir = Path("data")
    raw_file = data_dir / "nautica_raw.xlsx"
    starting_file = data_dir / "starting_values.json"
    output_file = data_dir / "nautica_processed.json"
    
    print("🔄 Processing Nautica data...")
    
    # ── Load today's daily data ────────────────────────────────────────────
    if not raw_file.exists():
        print(f"❌ Daily report not found: {raw_file}")
        sys.exit(1)
    
    print(f"📥 Reading daily report: {raw_file}")
    daily_data = parse_daily_report(raw_file)
    if daily_data is None:
        print("❌ No data to process")
        sys.exit(1)
    
    # Parse hourly arrays from same file
    hourly_arrays = parse_hourly_arrays(raw_file)
    data_hour = hourly_arrays['current_hour']
    
    # Show key daily values
    print(f"  ⚡ PV Yield today:      {daily_data.get('PV Yield (kWh)', 0):,.2f} kWh")
    print(f"  📤 Export today:         {daily_data.get('Export (kWh)', 0):,.2f} kWh")
    print(f"  📥 Import today:         {daily_data.get('Import (kWh)', 0):,.2f} kWh")
    
    # ── Recalculate Consumption from PV/Export/Import ──────────────────
    pv_today = daily_data.get('PV Yield (kWh)', 0.0)
    export_today = daily_data.get('Export (kWh)', 0.0)
    import_today = daily_data.get('Import (kWh)', 0.0)
    
    if pv_today <= 0:
        # No PV generation: Load = Import
        consumption_today = import_today
    elif export_today > 0:
        # PV generating & exporting: Load = PV - Export + Import
        consumption_today = pv_today - export_today + import_today
    else:
        # PV generating, no export: Load = PV + Import
        consumption_today = pv_today + import_today
    
    daily_data['Consumption (kWh)'] = round(consumption_today, 2)
    
    # Self-consumption = PV going to load (not exported)
    self_consumption_today = pv_today - export_today
    daily_data['Self-consumption (kWh)'] = round(max(0, self_consumption_today), 2)
    if pv_today > 0:
        daily_data['Self-consumption Rate (%)'] = round((self_consumption_today / pv_today) * 100, 2)
    
    print(f"  🏠 Consumption today:    {consumption_today:,.2f} kWh (calculated)")
    print(f"  🔌 Self-consumption:     {self_consumption_today:,.2f} kWh")
    print(f"  📤 To Grid (Export):     {export_today:,.2f} kWh")
    
    # ── Load starting values ───────────────────────────────────────────────
    if not starting_file.exists():
        print(f"❌ Starting values not found: {starting_file}")
        sys.exit(1)
    
    print(f"📥 Reading starting values: {starting_file}")
    with open(starting_file, "r") as f:
        starting = json.load(f)
    
    monthly = starting["monthly"]
    lifetime = starting["lifetime"]
    
    # ── Determine current month key ────────────────────────────────────────
    now = datetime.now(SAST)
    current_month_key = now.strftime("%Y-%m")
    current_year_key = now.strftime("%Y")
    today_str = now.strftime("%Y-%m-%d")
    
    print(f"📅 Current month: {current_month_key}")
    print(f"📅 Current year:  {current_year_key}")
    
    # ── Same-day re-run handling (prevent double-counting) ──────────────────
    last_run_date = starting.get("last_run_date", "")
    last_daily = starting.get("last_daily", {})
    month_seeded = starting.get("month_seeded", "")
    
    if current_month_key not in monthly:
        print(f"  ℹ️  New month {current_month_key} - starting fresh")
        monthly[current_month_key] = {}
    
    # If this month was seeded from authoritative data (includes today),
    # store today's daily for future same-day logic but don't add to monthly
    skip_add = False
    if month_seeded == current_month_key and last_run_date == today_str and not last_daily:
        print(f"  ℹ️  Month {current_month_key} seeded with today's data - storing daily, skipping add")
        starting["last_daily"] = dict(daily_data)
        starting["month_seeded"] = ""  # Clear flag after first run
        skip_add = True
    elif last_run_date == today_str and last_daily:
        # Same day: subtract previous daily so we don't double-count
        print(f"  🔄 Same-day re-run — subtracting previous daily before adding new")
        for field in ADDITIVE_FIELDS:
            prev = last_daily.get(field, 0.0)
            monthly[current_month_key][field] = monthly[current_month_key].get(field, 0.0) - prev
    elif last_run_date and last_run_date != today_str:
        print(f"  📅 New day: {last_run_date} → {today_str}")
        # Clear seeded flag on new day
        if month_seeded:
            starting["month_seeded"] = ""
    
    # ── Add today's daily to current month ──────────────────────────────────
    if not skip_add:
        print(f"📊 Updating monthly data for {current_month_key}...")
        monthly[current_month_key] = add_daily_to_month(
            monthly[current_month_key], daily_data
        )
    else:
        print(f"📊 Using seeded monthly data for {current_month_key} (no daily added)")
    
    month_pv = monthly[current_month_key].get("PV Yield (kWh)", 0)
    print(f"  ⚡ Month-to-date PV Yield: {month_pv:,.2f} kWh")
    
    # ── Recalculate lifetime for ALL years from monthly data ─────────────
    # This ensures any corrections to starting_values monthly data
    # (e.g. backfilled Self-consumption) flow through to lifetime
    all_years = sorted(set(k[:4] for k in monthly.keys()))
    for yr in all_years:
        year_totals = recalculate_lifetime_year(monthly, yr)
        if year_totals:
            # Preserve any lifetime-only fields (like Equivalent Trees Planted)
            if yr in lifetime:
                for key in lifetime[yr]:
                    if key not in year_totals:
                        year_totals[key] = lifetime[yr][key]
            lifetime[yr] = year_totals
    
    year_pv = lifetime.get(current_year_key, {}).get("PV Yield (kWh)", 0)
    print(f"  ⚡ Year-to-date PV Yield: {year_pv:,.2f} kWh")
    
    # ── Calculate all-time totals ──────────────────────────────────────────
    all_time = calculate_all_time_totals(lifetime)
    total_pv = all_time.get("PV Yield (kWh)", 0)
    print(f"  ⚡ All-time PV Yield:     {total_pv:,.2f} kWh")
    
    # ── Calculate Savings (PV TOU + Export Credits) ──────────────────────
    fin_config_file = data_dir.parent / "config" / "Financial config.json"
    pvsyst_file = data_dir.parent / "config" / "pvsyst_predictions.json"
    savings_out = {"today": {}, "current_month": {}, "all_time": {}}
    
    try:
        if fin_config_file.exists() and pvsyst_file.exists():
            with open(fin_config_file, "r") as f:
                fin = json.load(f)
            with open(pvsyst_file, "r") as f:
                pvs = json.load(f)
            
            rates = fin.get("rates", {})
            seasons = fin.get("seasons", {})
            tou_schedule = fin.get("tou_schedule", {})
            daily_hourly = pvs.get("daily_hourly", {})
            
            # Export offset credits from config (editable in Financial config.json)
            export_credits = fin.get("export_credits", {})
            print(f"  📋 Export credits: Std=R{export_credits.get('standard', 0)}, OffPk=R{export_credits.get('off_peak', 0)}")
            
            def get_tou_info(hour, date_obj):
                """Get TOU rate and period for a specific hour and date."""
                month_str = str(date_obj.month)
                season = seasons.get(month_str, "low_demand")
                weekday = date_obj.weekday()
                
                if weekday < 5:
                    day_type = "weekday"
                elif weekday == 5:
                    day_type = "saturday"
                else:
                    day_type = "sunday"
                
                schedule = tou_schedule.get(season, {}).get(day_type, [])
                period = schedule[hour] if hour < len(schedule) else "off_peak"
                rate = rates.get(season, {}).get(period, 0)
                return rate, period
            
            def calc_day_savings(self_cons_kwh, export_kwh, date_obj):
                """Calculate PV savings (TOU) and export credits for one day."""
                mmdd = date_obj.strftime("%m-%d")
                hourly_pattern = daily_hourly.get(mmdd, [0]*24)
                pattern_total = sum(hourly_pattern)
                
                pv_sav = {"peak": 0.0, "standard": 0.0, "off_peak": 0.0, "total": 0.0}
                exp_sav = {"standard": 0.0, "off_peak": 0.0, "total": 0.0}
                
                if pattern_total <= 0:
                    return pv_sav, exp_sav
                
                for h in range(24):
                    fraction = hourly_pattern[h] / pattern_total
                    rate, period = get_tou_info(h, date_obj)
                    
                    # PV savings: self-consumption × TOU rate
                    if self_cons_kwh > 0:
                        sc_kwh = self_cons_kwh * fraction
                        pv_sav[period] = pv_sav.get(period, 0) + sc_kwh * rate
                        pv_sav["total"] += sc_kwh * rate
                    
                    # Export credits: export × offset credit rate from config
                    if export_kwh > 0:
                        exp_kwh = export_kwh * fraction
                        credit_rate = export_credits.get(period, 0)
                        if credit_rate > 0:
                            exp_sav[period] = exp_sav.get(period, 0) + exp_kwh * credit_rate
                            exp_sav["total"] += exp_kwh * credit_rate
                
                pv_sav = {k: round(v, 2) for k, v in pv_sav.items()}
                exp_sav = {k: round(v, 2) for k, v in exp_sav.items()}
                return pv_sav, exp_sav
            
            # Today
            today_sc = daily_data.get('Self-consumption (kWh)', 0)
            today_exp = daily_data.get('Export (kWh)', 0)
            pv_s, exp_s = calc_day_savings(today_sc, today_exp, now)
            total_today = round(pv_s["total"] + exp_s["total"], 2)
            savings_out["today"] = {"pv_savings": pv_s, "export_savings": exp_s, "total": total_today}
            print(f"  💰 Today: PV=R{pv_s['total']:,.2f} + Export=R{exp_s['total']:,.2f} = R{total_today:,.2f}")
            
            # Monthly
            month_sc = monthly[current_month_key].get('Self-consumption (kWh)', 0)
            month_exp = monthly[current_month_key].get('Export (kWh)', 0)
            month_days_count = now.day
            if month_days_count > 0:
                month_pv = {"peak": 0.0, "standard": 0.0, "off_peak": 0.0, "total": 0.0}
                month_ex = {"standard": 0.0, "off_peak": 0.0, "total": 0.0}
                daily_avg_sc = month_sc / month_days_count
                daily_avg_exp = month_exp / month_days_count
                for d in range(1, month_days_count + 1):
                    day_date = now.replace(day=d)
                    dp, de = calc_day_savings(daily_avg_sc, daily_avg_exp, day_date)
                    for k in month_pv: month_pv[k] += dp.get(k, 0)
                    for k in month_ex: month_ex[k] += de.get(k, 0)
                month_pv = {k: round(v, 2) for k, v in month_pv.items()}
                month_ex = {k: round(v, 2) for k, v in month_ex.items()}
                total_month = round(month_pv["total"] + month_ex["total"], 2)
                savings_out["current_month"] = {"pv_savings": month_pv, "export_savings": month_ex, "total": total_month}
            print(f"  💰 Month: PV=R{savings_out['current_month'].get('pv_savings',{}).get('total',0):,.2f} + Export=R{savings_out['current_month'].get('export_savings',{}).get('total',0):,.2f}")
            
            # Lifetime: loop through every historical month properly
            lt_pv = {"peak": 0.0, "standard": 0.0, "off_peak": 0.0, "total": 0.0}
            lt_ex = {"standard": 0.0, "off_peak": 0.0, "total": 0.0}
            
            for mk, mv in monthly.items():
                m_sc = mv.get('Self-consumption (kWh)', 0)
                m_exp = mv.get('Export (kWh)', 0)
                if m_sc <= 0 and m_exp <= 0:
                    continue
                
                # Parse year-month
                try:
                    parts = mk.split('-')
                    m_year, m_month = int(parts[0]), int(parts[1])
                except (ValueError, IndexError):
                    continue
                
                # For current month: use days elapsed; for past months: full month
                if mk == current_month_key:
                    num_days = now.day
                else:
                    num_days = calendar.monthrange(m_year, m_month)[1]
                
                if num_days <= 0:
                    continue
                
                daily_sc = m_sc / num_days
                daily_exp = m_exp / num_days
                
                # Loop each actual calendar day for correct weekday/weekend TOU
                for d in range(1, num_days + 1):
                    try:
                        day_date = datetime(m_year, m_month, d, tzinfo=SAST)
                    except ValueError:
                        continue
                    dp, de = calc_day_savings(daily_sc, daily_exp, day_date)
                    for k in lt_pv: lt_pv[k] += dp.get(k, 0)
                    for k in lt_ex: lt_ex[k] += de.get(k, 0)
            
            lt_pv = {k: round(v, 2) for k, v in lt_pv.items()}
            lt_ex = {k: round(v, 2) for k, v in lt_ex.items()}
            total_lt = round(lt_pv["total"] + lt_ex["total"], 2)
            savings_out["all_time"] = {"pv_savings": lt_pv, "export_savings": lt_ex, "total": total_lt}
            print(f"  💰 Lifetime: PV=R{lt_pv['total']:,.2f} + Export=R{lt_ex['total']:,.2f} = R{total_lt:,.2f}")
        else:
            if not fin_config_file.exists():
                print("  ℹ️  Financial config not found - skipping savings")
            if not pvsyst_file.exists():
                print("  ℹ️  PVSyst predictions not found - skipping savings")
    except Exception as e:
        print(f"  ⚠️  Savings calc error (non-fatal): {e}")
        import traceback; traceback.print_exc()
    
    # ── Load yesterday's data ─────────────────────────────────────────────
    # Check new key first, fall back to old key for backward compatibility
    yesterday_data = starting.get("yesterday", None)
    yesterday_date = starting.get("yesterday_date", "")
    
    # Backward compat: if old key exists but new key doesn't, migrate
    if yesterday_data is None and "previous_today" in starting:
        prev_date = starting.get("previous_today_date", "")
        today_date = now.strftime("%Y-%m-%d")
        if prev_date and prev_date != today_date:
            # The old previous_today is actually yesterday's data
            yesterday_data = starting["previous_today"]
            yesterday_date = prev_date
            print(f"  📅 Migrated yesterday from previous_today ({prev_date})")
    
    # ── Build output ───────────────────────────────────────────────────────
    output = {
        "plant": "Nautica Shopping Centre",
        "last_updated": now.strftime("%Y-%m-%d %H:%M"),
        "yesterday": {
            "date": yesterday_date,
            "data": {k: round(v, 2) for k, v in yesterday_data.items()}
        } if yesterday_data else None,
        "today": {
            "date": now.strftime("%Y-%m-%d"),
            "data": {k: round(v, 2) for k, v in daily_data.items()}
        },
        "current_month": {
            "period": current_month_key,
            "data": {k: round(v, 2) for k, v in monthly[current_month_key].items()}
        },
        "monthly": {
            k: {fk: round(fv, 2) for fk, fv in v.items()}
            for k, v in sorted(monthly.items())
        },
        "lifetime": {
            k: {fk: round(fv, 2) for fk, fv in v.items()}
            for k, v in sorted(lifetime.items())
        },
        "all_time_totals": {k: round(v, 2) for k, v in all_time.items()},
        "savings": savings_out
    }
    
    # ── Save output ────────────────────────────────────────────────────────
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"✅ Output saved to: {output_file}")
    
    # ── Hourly generation tracking ──────────────────────────────────────────
    hourly_file = data_dir / "hourly_generation.json"
    try:
        if hourly_file.exists():
            with open(hourly_file, "r") as f:
                hourly_gen = json.load(f)
        else:
            hourly_gen = {"days": {}, "days_load": {}, "days_grid": {}}
        
        today_date = today_str
        
        # Store actual hourly arrays from xlsx
        hourly_gen["days"][today_date] = hourly_arrays['pv']
        if "days_load" not in hourly_gen:
            hourly_gen["days_load"] = {}
        if "days_grid" not in hourly_gen:
            hourly_gen["days_grid"] = {}
        hourly_gen["days_load"][today_date] = hourly_arrays['load']
        hourly_gen["days_grid"][today_date] = hourly_arrays['import']
        
        # Monthly averages for load and grid (include today)
        current_month_prefix = now.strftime("%Y-%m")
        month_load_days = {d: hrs for d, hrs in hourly_gen.get("days_load", {}).items()
                          if d.startswith(current_month_prefix)}
        month_grid_days = {d: hrs for d, hrs in hourly_gen.get("days_grid", {}).items()
                          if d.startswith(current_month_prefix)}
        month_pv_days = {d: hrs for d, hrs in hourly_gen.get("days", {}).items()
                        if d.startswith(current_month_prefix)}
        
        avg_load = [0.0] * 24
        avg_grid = [0.0] * 24
        avg_pv = [0.0] * 24
        if month_load_days:
            for hour in range(24):
                load_vals = [hrs[hour] for hrs in month_load_days.values() if hour < len(hrs)]
                avg_load[hour] = round(sum(load_vals) / len(load_vals), 2) if load_vals else 0.0
        if month_grid_days:
            for hour in range(24):
                grid_vals = [hrs[hour] for hrs in month_grid_days.values() if hour < len(hrs)]
                avg_grid[hour] = round(sum(grid_vals) / len(grid_vals), 2) if grid_vals else 0.0
        if month_pv_days:
            for hour in range(24):
                pv_vals = [hrs[hour] for hrs in month_pv_days.values() if hour < len(hrs)]
                avg_pv[hour] = round(sum(pv_vals) / len(pv_vals), 2) if pv_vals else 0.0
        
        # Prune old data (keep last 90 days)
        cutoff = (now - timedelta(days=90)).strftime("%Y-%m-%d")
        hourly_gen["days"] = {d: v for d, v in hourly_gen["days"].items() if d >= cutoff}
        hourly_gen["days_load"] = {d: v for d, v in hourly_gen.get("days_load", {}).items() if d >= cutoff}
        hourly_gen["days_grid"] = {d: v for d, v in hourly_gen.get("days_grid", {}).items() if d >= cutoff}
        
        with open(hourly_file, "w") as f:
            json.dump(hourly_gen, f, indent=2)
        print(f"✅ Hourly arrays stored: PV peak={max(hourly_arrays['pv']):.1f} kW at hour {data_hour}")
        
    except Exception as e:
        print(f"⚠️  Hourly tracking error (non-fatal): {e}")
        avg_load = [0.0] * 24
        avg_grid = [0.0] * 24
        avg_pv = [0.0] * 24
    
    # ── Add hourly data to output ───────────────────────────────────────────
    try:
        output["hourly"] = {
            "current_hour": data_hour,
            "pv": hourly_arrays['pv'],
            "load": hourly_arrays['load'],
            "grid": hourly_arrays['import'],
            "export": hourly_arrays['export'],
            "avg_load": avg_load,
            "avg_grid": avg_grid,
            "avg_pv": avg_pv
        }
        # Re-save output with hourly data
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)
    except Exception as e:
        print(f"⚠️  Hourly output error (non-fatal): {e}")
    
    # ── Daily history accumulation ──────────────────────────────────────────
    daily_hist_file = data_dir / "daily_history.json"
    try:
        if daily_hist_file.exists():
            with open(daily_hist_file, "r") as f:
                daily_hist = json.load(f)
        else:
            daily_hist = {}
        
        # Calculate per-TOU-period breakdown from actual hourly data
        tou_breakdown = {}
        if fin_config_file.exists():
            with open(fin_config_file, "r") as f:
                fin_cfg = json.load(f)
            d_rates = fin_cfg.get("rates", {})
            d_seasons = fin_cfg.get("seasons", {})
            d_schedule = fin_cfg.get("tou_schedule", {})
            d_export_cr = fin_cfg.get("export_credits", {})
            
            month_str = str(now.month)
            season = d_seasons.get(month_str, "low_demand")
            wd = now.weekday()
            day_type = "weekday" if wd < 5 else ("saturday" if wd == 5 else "sunday")
            schedule = d_schedule.get(season, {}).get(day_type, [])
            
            for period in ["peak", "standard", "off_peak"]:
                tou_breakdown[period] = {
                    "generation": 0, "import": 0, "self_consumption": 0,
                    "export": 0, "utility_cost": 0, "pv_savings": 0, "export_savings": 0
                }
            
            for h in range(24):
                period = schedule[h] if h < len(schedule) else "off_peak"
                rate = d_rates.get(season, {}).get(period, 0)
                credit = d_export_cr.get(period, 0)
                
                pv_h = hourly_arrays['pv'][h]
                imp_h = hourly_arrays['import'][h]
                exp_h = hourly_arrays['export'][h]
                # Self-consumption = PV - Export (per hour)
                sc_h = max(0, pv_h - exp_h)
                
                tb = tou_breakdown[period]
                tb["generation"] += pv_h
                tb["import"] += imp_h
                tb["self_consumption"] += sc_h
                tb["export"] += exp_h
                tb["utility_cost"] += imp_h * rate
                tb["pv_savings"] += sc_h * rate
                tb["export_savings"] += exp_h * credit
            
            # Round
            for period in tou_breakdown:
                tou_breakdown[period] = {k: round(v, 2) for k, v in tou_breakdown[period].items()}
        
        # Build today's record
        day_record = {
            "current_hour": data_hour,
            "pv": round(daily_data.get("PV Yield (kWh)", 0), 2),
            "import": round(daily_data.get("Import (kWh)", 0), 2),
            "export": round(daily_data.get("Export (kWh)", 0), 2),
            "self_consumption": round(daily_data.get("Self-consumption (kWh)", 0), 2),
            "consumption": round(consumption_today, 2),
            "hourly": {
                "pv": hourly_arrays['pv'],
                "load": hourly_arrays['load'],
                "grid": hourly_arrays['import'],
                "export": hourly_arrays['export']
            },
            "tou_breakdown": tou_breakdown,
            "savings": savings_out.get("today", {})
        }
        
        daily_hist[today_str] = day_record
        
        # Keep last 365 days
        if len(daily_hist) > 365:
            cutoff_date = (now - timedelta(days=365)).strftime("%Y-%m-%d")
            daily_hist = {d: v for d, v in daily_hist.items() if d >= cutoff_date}
        
        with open(daily_hist_file, "w") as f:
            json.dump(daily_hist, f, indent=2)
        print(f"✅ Daily history: {len(daily_hist)} days stored ({today_str} updated)")
        
    except Exception as e:
        print(f"⚠️  Daily history error (non-fatal): {e}")
        import traceback; traceback.print_exc()
    
    # ── Update starting values for next run ────────────────────────────────
    starting["monthly"] = monthly
    starting["lifetime"] = lifetime
    starting["last_updated"] = now.strftime("%Y-%m-%d")
    starting["last_run_date"] = today_str
    starting["last_daily"] = {field: daily_data.get(field, 0.0) for field in ADDITIVE_FIELDS}
    
    # Only rotate today→yesterday when the date actually changes
    prev_today_date = starting.get("previous_today_date", "")
    
    if prev_today_date and prev_today_date != today_str:
        # Date changed — yesterday becomes the final snapshot from previous day
        starting["yesterday"] = starting.get("previous_today", {})
        starting["yesterday_date"] = prev_today_date
        print(f"  📅 Rotated yesterday: {prev_today_date}")
    
    # Always update today's running snapshot
    starting["previous_today"] = daily_data
    starting["previous_today_date"] = today_str
    
    with open(starting_file, "w") as f:
        json.dump(starting, f, indent=2)
    print(f"✅ Starting values updated: {starting_file}")
    
    print("✅ Processing complete!")


if __name__ == "__main__":
    main()
