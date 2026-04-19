"""
process_nautica_data.py (DEMO VERSION)

Same as production processor but applies boost factors to make
performance % and coverage % look better for demo purposes.

Strategy:
  - Increase PV Yield by 8% → better performance (actual/expected)
  - Reduce Consumption/Load by 15% → better coverage (SC/Load)
  - Self-consumption increases proportionally (more PV, less load)
  - Grid import decreases (less load needed from grid)
  - Export increases slightly (excess PV)

These factors are applied to BOTH incoming daily data AND
when recalculating from starting_values, so historical months
also show improved numbers.
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

# ══════════════════════════════════════════════════
# DEMO BOOST FACTORS — adjust these to tune results
# ══════════════════════════════════════════════════
PV_BOOST = 1.08        # +8% PV generation
LOAD_REDUCTION = 0.85  # -15% consumption/load
# Derived: SC increases because more PV covers reduced load
# Derived: Export increases because surplus PV after reduced load
# Derived: Grid import decreases because less load to fill

def apply_boost(daily_data):
    """Apply demo boost factors to a daily data dict."""
    boosted = dict(daily_data)

    # Original values
    orig_pv = boosted.get('PV Yield (kWh)', 0)
    orig_export = boosted.get('Export (kWh)', 0)
    orig_import = boosted.get('Import (kWh)', 0)

    # Boost PV
    new_pv = orig_pv * PV_BOOST
    boosted['PV Yield (kWh)'] = round(new_pv, 3)

    # Boosted inverter yield too
    if 'Inverter Yield (kWh)' in boosted:
        boosted['Inverter Yield (kWh)'] = round(boosted['Inverter Yield (kWh)'] * PV_BOOST, 3)
    if 'Theoretical Yield (kWh)' in boosted:
        boosted['Theoretical Yield (kWh)'] = round(boosted['Theoretical Yield (kWh)'] * PV_BOOST, 3)

    # Recalculate energy balance with reduced load
    orig_consumption = orig_pv - orig_export + orig_import  # original load
    new_consumption = orig_consumption * LOAD_REDUCTION

    # New self-consumption = min(new_pv, new_consumption)
    new_sc = min(new_pv, new_consumption)
    new_export = max(0, new_pv - new_sc)
    new_import = max(0, new_consumption - new_sc)

    boosted['Consumption (kWh)'] = round(new_consumption, 3)
    boosted['Self-consumption (kWh)'] = round(new_sc, 3)
    boosted['Export (kWh)'] = round(new_export, 3)
    boosted['Import (kWh)'] = round(new_import, 3)

    if new_pv > 0:
        boosted['Self-consumption Rate (%)'] = round((new_sc / new_pv) * 100, 3)

    # Boost CO2/coal/revenue proportionally to PV increase
    for field in ['CO₂ Avoided (t)', 'Standard Coal Saved (t)', 'Revenue (R.)']:
        if field in boosted and boosted[field] > 0:
            boosted[field] = round(boosted[field] * PV_BOOST, 3)

    return boosted

def apply_boost_hourly(hourly_arrays):
    """Apply boost to hourly arrays."""
    boosted = dict(hourly_arrays)
    boosted['pv'] = [round(v * PV_BOOST, 2) for v in hourly_arrays['pv']]

    # Recalculate load/grid/export per hour
    new_load = []
    new_grid = []
    new_export = []
    for h in range(24):
        pv = boosted['pv'][h]
        orig_load_h = hourly_arrays['load'][h]
        reduced_load = orig_load_h * LOAD_REDUCTION

        sc_h = min(pv, reduced_load)
        exp_h = max(0, pv - sc_h)
        imp_h = max(0, reduced_load - sc_h)

        new_load.append(round(reduced_load, 2))
        new_grid.append(round(imp_h, 2))
        new_export.append(round(exp_h, 2))

    boosted['load'] = new_load
    boosted['import'] = new_grid
    boosted['export'] = new_export
    return boosted

def apply_boost_monthly(monthly_data):
    """Apply boost to an entire monthly totals dict."""
    return apply_boost(monthly_data)


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

MAX_FIELDS = [
    "Peak Power (kW)",
]


def parse_daily_report(filepath):
    df = pd.read_excel(filepath, header=None, sheet_name=0)
    headers = df.iloc[1].tolist()
    combined = {}
    row_count = 0
    for idx in range(2, len(df)):
        row = df.iloc[idx]
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
        return None
    print(f"  ✅ Parsed {row_count} row(s) from daily report")
    return combined


def parse_hourly_arrays(filepath):
    df = pd.read_excel(filepath, header=None, sheet_name=0)
    headers = [str(h).strip() if not pd.isna(h) else '' for h in df.iloc[1].tolist()]
    pv_col = next((i for i, h in enumerate(headers) if h == 'PV Yield (kWh)'), None)
    exp_col = next((i for i, h in enumerate(headers) if h == 'Export (kWh)'), None)
    imp_col = next((i for i, h in enumerate(headers) if h == 'Import (kWh)'), None)
    pv_arr = [0.0]*24; imp_arr = [0.0]*24; exp_arr = [0.0]*24; load_arr = [0.0]*24
    current_hour = 0
    for idx in range(2, len(df)):
        row = df.iloc[idx]
        try:
            ts = pd.Timestamp(row.iloc[0]); hour = ts.hour
        except: continue
        pv = float(row.iloc[pv_col]) if pv_col is not None and not pd.isna(row.iloc[pv_col]) else 0.0
        exp = float(row.iloc[exp_col]) if exp_col is not None and not pd.isna(row.iloc[exp_col]) else 0.0
        imp = float(row.iloc[imp_col]) if imp_col is not None and not pd.isna(row.iloc[imp_col]) else 0.0
        load = imp if pv <= 0 else (pv - exp + imp if exp > 0 else pv + imp)
        pv_arr[hour] = round(pv, 2); imp_arr[hour] = round(imp, 2)
        exp_arr[hour] = round(exp, 2); load_arr[hour] = round(load, 2)
        current_hour = hour
    return {'current_hour': current_hour, 'pv': pv_arr, 'import': imp_arr, 'export': exp_arr, 'load': load_arr}


def add_daily_to_month(monthly_data, daily_data):
    updated = dict(monthly_data)
    for field in ADDITIVE_FIELDS:
        updated[field] = round(updated.get(field, 0.0) + daily_data.get(field, 0.0), 3)
    for field in MAX_FIELDS:
        updated[field] = round(max(updated.get(field, 0.0), daily_data.get(field, 0.0)), 3)
    pv = updated.get("PV Yield (kWh)", 0.0)
    sc = updated.get("Self-consumption (kWh)", 0.0)
    if pv > 0:
        updated["Self-consumption Rate (%)"] = round((sc / pv) * 100, 3)
    return updated


def recalculate_lifetime_year(monthly_data, year_str):
    matching = {k: v for k, v in monthly_data.items() if k.startswith(year_str)}
    if not matching: return None
    year_total = {}
    for mv in matching.values():
        for field in ADDITIVE_FIELDS:
            year_total[field] = year_total.get(field, 0.0) + mv.get(field, 0.0)
        for field in MAX_FIELDS:
            year_total[field] = max(year_total.get(field, 0.0), mv.get(field, 0.0))
    for key in year_total: year_total[key] = round(year_total[key], 3)
    pv = year_total.get("PV Yield (kWh)", 0.0)
    sc = year_total.get("Self-consumption (kWh)", 0.0)
    if pv > 0:
        year_total["Self-consumption Rate (%)"] = round((sc / pv) * 100, 3)
    return year_total


def calculate_all_time_totals(lifetime_data):
    totals = {}
    for yv in lifetime_data.values():
        for field in ADDITIVE_FIELDS:
            totals[field] = totals.get(field, 0.0) + yv.get(field, 0.0)
        for field in MAX_FIELDS:
            totals[field] = max(totals.get(field, 0.0), yv.get(field, 0.0))
    for key in totals: totals[key] = round(totals[key], 3)
    pv = totals.get("PV Yield (kWh)", 0.0)
    sc = totals.get("Self-consumption (kWh)", 0.0)
    if pv > 0:
        totals["Self-consumption Rate (%)"] = round((sc / pv) * 100, 3)
    return totals


def main():
    data_dir = Path("data")
    raw_file = data_dir / "nautica_raw.xlsx"
    starting_file = data_dir / "starting_values.json"
    output_file = data_dir / "nautica_processed.json"

    print("🔄 Processing Nautica data (DEMO MODE)...")
    print(f"   PV Boost: +{(PV_BOOST-1)*100:.0f}%  |  Load Reduction: -{(1-LOAD_REDUCTION)*100:.0f}%")

    if not raw_file.exists():
        print(f"❌ Daily report not found: {raw_file}")
        sys.exit(1)

    print(f"📥 Reading daily report: {raw_file}")
    daily_data = parse_daily_report(raw_file)
    if daily_data is None:
        print("❌ No data to process"); sys.exit(1)

    hourly_arrays = parse_hourly_arrays(raw_file)
    data_hour = hourly_arrays['current_hour']

    # ── APPLY BOOST to daily data ──
    pv_today = daily_data.get('PV Yield (kWh)', 0)
    export_today = daily_data.get('Export (kWh)', 0)
    import_today = daily_data.get('Import (kWh)', 0)
    consumption_today = import_today if pv_today <= 0 else (pv_today - export_today + import_today if export_today > 0 else pv_today + import_today)
    daily_data['Consumption (kWh)'] = round(consumption_today, 2)
    self_consumption_today = pv_today - export_today
    daily_data['Self-consumption (kWh)'] = round(max(0, self_consumption_today), 2)
    if pv_today > 0:
        daily_data['Self-consumption Rate (%)'] = round((self_consumption_today / pv_today) * 100, 2)

    print(f"  ⚡ Raw PV: {pv_today:,.2f} kWh | Raw Load: {consumption_today:,.2f} kWh")

    # BOOST daily data
    daily_data = apply_boost(daily_data)
    hourly_arrays = apply_boost_hourly(hourly_arrays)

    print(f"  ⚡ Boosted PV: {daily_data.get('PV Yield (kWh)',0):,.2f} kWh | Boosted Load: {daily_data.get('Consumption (kWh)',0):,.2f} kWh")

    # ── Load starting values ──
    if not starting_file.exists():
        print(f"❌ Starting values not found: {starting_file}"); sys.exit(1)

    with open(starting_file, "r") as f:
        starting = json.load(f)

    monthly = starting["monthly"]
    lifetime = starting["lifetime"]

    # ── BOOST ALL HISTORICAL MONTHLY DATA ──
    print("📊 Applying boost to all historical months...")
    for mk in monthly:
        monthly[mk] = apply_boost_monthly(monthly[mk])

    now = datetime.now(SAST)
    current_month_key = now.strftime("%Y-%m")
    current_year_key = now.strftime("%Y")
    today_str = now.strftime("%Y-%m-%d")

    last_run_date = starting.get("last_run_date", "")
    last_daily = starting.get("last_daily", {})
    month_seeded = starting.get("month_seeded", "")

    if current_month_key not in monthly:
        monthly[current_month_key] = {}

    skip_add = False
    if month_seeded == current_month_key and last_run_date == today_str and not last_daily:
        starting["last_daily"] = dict(daily_data)
        starting["month_seeded"] = ""
        skip_add = True
    elif last_run_date == today_str and last_daily:
        # Same day: subtract previous boosted daily
        boosted_last = apply_boost(last_daily) if last_daily else {}
        for field in ADDITIVE_FIELDS:
            prev = boosted_last.get(field, 0.0)
            monthly[current_month_key][field] = monthly[current_month_key].get(field, 0.0) - prev
    elif last_run_date and last_run_date != today_str:
        if month_seeded: starting["month_seeded"] = ""

    if not skip_add:
        monthly[current_month_key] = add_daily_to_month(monthly[current_month_key], daily_data)

    # ── Recalculate lifetime ──
    all_years = sorted(set(k[:4] for k in monthly.keys()))
    for yr in all_years:
        year_totals = recalculate_lifetime_year(monthly, yr)
        if year_totals:
            if yr in lifetime:
                for key in lifetime[yr]:
                    if key not in year_totals: year_totals[key] = lifetime[yr][key]
            lifetime[yr] = year_totals

    all_time = calculate_all_time_totals(lifetime)
    total_pv = all_time.get("PV Yield (kWh)", 0)
    print(f"  ⚡ All-time PV Yield (boosted): {total_pv:,.2f} kWh")

    # ── Savings ──
    fin_config_file = data_dir.parent / "config" / "Financial config.json"
    pvsyst_file = data_dir.parent / "config" / "pvsyst_predictions.json"
    savings_out = {"today": {}, "current_month": {}, "all_time": {}}

    try:
        if fin_config_file.exists() and pvsyst_file.exists():
            with open(fin_config_file, "r") as f: fin = json.load(f)
            with open(pvsyst_file, "r") as f: pvs = json.load(f)
            rates = fin.get("rates", {})
            seasons = fin.get("seasons", {})
            tou_schedule = fin.get("tou_schedule", {})
            daily_hourly = pvs.get("daily_hourly", {})
            export_credits = fin.get("export_credits", {})

            def get_tou_info(hour, date_obj):
                season = seasons.get(str(date_obj.month), "low_demand")
                wd = date_obj.weekday()
                day_type = "weekday" if wd < 5 else ("saturday" if wd == 5 else "sunday")
                schedule = tou_schedule.get(season, {}).get(day_type, [])
                period = schedule[hour] if hour < len(schedule) else "off_peak"
                rate = rates.get(season, {}).get(period, 0)
                return rate, period

            def calc_day_savings(sc_kwh, exp_kwh, date_obj):
                mmdd = date_obj.strftime("%m-%d")
                hp = daily_hourly.get(mmdd, [0]*24)
                pt = sum(hp)
                pv_s = {"peak":0,"standard":0,"off_peak":0,"total":0}
                exp_s = {"standard":0,"off_peak":0,"total":0}
                if pt <= 0: return pv_s, exp_s
                for h in range(24):
                    frac = hp[h]/pt
                    rate, period = get_tou_info(h, date_obj)
                    if sc_kwh > 0:
                        s = sc_kwh*frac*rate; pv_s[period]+=s; pv_s["total"]+=s
                    if exp_kwh > 0:
                        cr = export_credits.get(period, 0)
                        if cr > 0:
                            e = exp_kwh*frac*cr; exp_s[period]+=e; exp_s["total"]+=e
                return {k:round(v,2) for k,v in pv_s.items()}, {k:round(v,2) for k,v in exp_s.items()}

            # Today
            t_sc = daily_data.get('Self-consumption (kWh)', 0)
            t_exp = daily_data.get('Export (kWh)', 0)
            ps, es = calc_day_savings(t_sc, t_exp, now)
            savings_out["today"] = {"pv_savings":ps,"export_savings":es,"total":round(ps["total"]+es["total"],2)}

            # Month
            m_sc = monthly[current_month_key].get('Self-consumption (kWh)', 0)
            m_exp = monthly[current_month_key].get('Export (kWh)', 0)
            nd = now.day
            if nd > 0:
                mp={"peak":0,"standard":0,"off_peak":0,"total":0}; me={"standard":0,"off_peak":0,"total":0}
                for d in range(1, nd+1):
                    dp,de = calc_day_savings(m_sc/nd, m_exp/nd, now.replace(day=d))
                    for k in mp: mp[k]+=dp.get(k,0)
                    for k in me: me[k]+=de.get(k,0)
                mp={k:round(v,2) for k,v in mp.items()}; me={k:round(v,2) for k,v in me.items()}
                savings_out["current_month"] = {"pv_savings":mp,"export_savings":me,"total":round(mp["total"]+me["total"],2)}

            # Lifetime
            lp={"peak":0,"standard":0,"off_peak":0,"total":0}; le={"standard":0,"off_peak":0,"total":0}
            for mk, mv in monthly.items():
                msc=mv.get('Self-consumption (kWh)',0); mex=mv.get('Export (kWh)',0)
                if msc<=0 and mex<=0: continue
                try: parts=mk.split('-'); my,mm=int(parts[0]),int(parts[1])
                except: continue
                ndays = now.day if mk==current_month_key else calendar.monthrange(my,mm)[1]
                if ndays<=0: continue
                for d in range(1, ndays+1):
                    try: dd=datetime(my,mm,d,tzinfo=SAST)
                    except: continue
                    dp,de = calc_day_savings(msc/ndays, mex/ndays, dd)
                    for k in lp: lp[k]+=dp.get(k,0)
                    for k in le: le[k]+=de.get(k,0)
            lp={k:round(v,2) for k,v in lp.items()}; le={k:round(v,2) for k,v in le.items()}
            savings_out["all_time"]={"pv_savings":lp,"export_savings":le,"total":round(lp["total"]+le["total"],2)}
            print(f"  💰 Lifetime savings (boosted): R{savings_out['all_time']['total']:,.2f}")
    except Exception as e:
        print(f"  ⚠️  Savings calc error: {e}")

    # ── Yesterday data ──
    yesterday_data = starting.get("yesterday", None)
    yesterday_date = starting.get("yesterday_date", "")
    if yesterday_data is None and "previous_today" in starting:
        prev_date = starting.get("previous_today_date", "")
        if prev_date and prev_date != now.strftime("%Y-%m-%d"):
            yesterday_data = starting["previous_today"]
            yesterday_date = prev_date

    # Boost yesterday too
    if yesterday_data:
        yesterday_data = apply_boost(yesterday_data)

    # ── Build output ──
    output = {
        "plant": "Nautica Shopping Centre",
        "last_updated": now.strftime("%Y-%m-%d %H:%M"),
        "yesterday": {"date": yesterday_date, "data": {k:round(v,2) for k,v in yesterday_data.items()}} if yesterday_data else None,
        "today": {"date": now.strftime("%Y-%m-%d"), "data": {k:round(v,2) for k,v in daily_data.items()}},
        "current_month": {"period": current_month_key, "data": {k:round(v,2) for k,v in monthly[current_month_key].items()}},
        "monthly": {k:{fk:round(fv,2) for fk,fv in v.items()} for k,v in sorted(monthly.items())},
        "lifetime": {k:{fk:round(fv,2) for fk,fv in v.items()} for k,v in sorted(lifetime.items())},
        "all_time_totals": {k:round(v,2) for k,v in all_time.items()},
        "savings": savings_out
    }

    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"✅ Output saved to: {output_file}")

    # ── Hourly tracking ──
    hourly_file = data_dir / "hourly_generation.json"
    avg_load = [0.0]*24; avg_grid = [0.0]*24; avg_pv = [0.0]*24
    try:
        hourly_gen = json.load(open(hourly_file)) if hourly_file.exists() else {"days":{},"days_load":{},"days_grid":{}}
        hourly_gen["days"][today_str] = hourly_arrays['pv']
        hourly_gen.setdefault("days_load",{})[today_str] = hourly_arrays['load']
        hourly_gen.setdefault("days_grid",{})[today_str] = hourly_arrays['import']
        cp = now.strftime("%Y-%m")
        for arr_key, avg_arr in [("days_load",avg_load),("days_grid",avg_grid),("days",avg_pv)]:
            md = {d:h for d,h in hourly_gen.get(arr_key,{}).items() if d.startswith(cp)}
            if md:
                for h in range(24):
                    vals = [hrs[h] for hrs in md.values() if h < len(hrs)]
                    avg_arr[h] = round(sum(vals)/len(vals),2) if vals else 0
        cutoff = (now-timedelta(days=90)).strftime("%Y-%m-%d")
        for k in ["days","days_load","days_grid"]:
            hourly_gen[k] = {d:v for d,v in hourly_gen.get(k,{}).items() if d >= cutoff}
        json.dump(hourly_gen, open(hourly_file,"w"), indent=2)
    except Exception as e:
        print(f"⚠️  Hourly tracking error: {e}")

    # Add hourly to output
    output["hourly"] = {
        "current_hour": data_hour,
        "pv": hourly_arrays['pv'], "load": hourly_arrays['load'],
        "grid": hourly_arrays['import'], "export": hourly_arrays['export'],
        "avg_load": avg_load, "avg_grid": avg_grid, "avg_pv": avg_pv
    }
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    # ── Daily history ──
    daily_hist_file = data_dir / "daily_history.json"
    try:
        daily_hist = json.load(open(daily_hist_file)) if daily_hist_file.exists() else {}

        # Boost ALL historical daily records
        for dk in daily_hist:
            rec = daily_hist[dk]
            if 'pv' in rec:
                rec['pv'] = round(rec['pv'] * PV_BOOST, 2)
            if 'consumption' in rec and rec['consumption'] > 0:
                rec['consumption'] = round(rec['consumption'] * LOAD_REDUCTION, 2)
            if 'self_consumption' in rec:
                rec['self_consumption'] = round(min(rec.get('pv',0), rec['consumption'] if 'consumption' in rec else rec.get('self_consumption',0)), 2)
            if 'export' in rec:
                rec['export'] = round(max(0, rec.get('pv',0) - rec.get('self_consumption',0)), 2)
            if 'import' in rec:
                rec['import'] = round(max(0, rec.get('consumption',0) - rec.get('self_consumption',0)), 2)
            # Boost hourly arrays if present
            if 'hourly' in rec:
                if 'pv' in rec['hourly']:
                    rec['hourly']['pv'] = [round(v*PV_BOOST,2) for v in rec['hourly']['pv']]
                if 'load' in rec['hourly']:
                    rec['hourly']['load'] = [round(v*LOAD_REDUCTION,2) for v in rec['hourly']['load']]

        # Add today (already boosted)
        tou_breakdown = {}
        try:
            if fin_config_file.exists():
                fin_cfg = json.load(open(fin_config_file))
                d_rates=fin_cfg.get("rates",{}); d_seasons=fin_cfg.get("seasons",{})
                d_schedule=fin_cfg.get("tou_schedule",{}); d_export_cr=fin_cfg.get("export_credits",{})
                season=d_seasons.get(str(now.month),"low_demand")
                wd=now.weekday(); day_type="weekday" if wd<5 else ("saturday" if wd==5 else "sunday")
                schedule=d_schedule.get(season,{}).get(day_type,[])
                for p in ["peak","standard","off_peak"]:
                    tou_breakdown[p]={"generation":0,"import":0,"self_consumption":0,"export":0,"utility_cost":0,"pv_savings":0,"export_savings":0}
                for h in range(24):
                    period=schedule[h] if h<len(schedule) else "off_peak"
                    rate=d_rates.get(season,{}).get(period,0); credit=d_export_cr.get(period,0)
                    pv_h=hourly_arrays['pv'][h]; imp_h=hourly_arrays['import'][h]; exp_h=hourly_arrays['export'][h]
                    sc_h=max(0,pv_h-exp_h)
                    tb=tou_breakdown[period]
                    tb["generation"]+=pv_h; tb["import"]+=imp_h; tb["self_consumption"]+=sc_h
                    tb["export"]+=exp_h; tb["utility_cost"]+=imp_h*rate; tb["pv_savings"]+=sc_h*rate; tb["export_savings"]+=exp_h*credit
                for p in tou_breakdown: tou_breakdown[p]={k:round(v,2) for k,v in tou_breakdown[p].items()}
        except: pass

        daily_hist[today_str] = {
            "current_hour": data_hour,
            "pv": round(daily_data.get("PV Yield (kWh)",0),2),
            "import": round(daily_data.get("Import (kWh)",0),2),
            "export": round(daily_data.get("Export (kWh)",0),2),
            "self_consumption": round(daily_data.get("Self-consumption (kWh)",0),2),
            "consumption": round(daily_data.get("Consumption (kWh)",0),2),
            "hourly": {"pv":hourly_arrays['pv'],"load":hourly_arrays['load'],"grid":hourly_arrays['import'],"export":hourly_arrays['export']},
            "tou_breakdown": tou_breakdown,
            "savings": savings_out.get("today",{})
        }
        if len(daily_hist)>365:
            cutoff_date=(now-timedelta(days=365)).strftime("%Y-%m-%d")
            daily_hist={d:v for d,v in daily_hist.items() if d>=cutoff_date}
        json.dump(daily_hist, open(daily_hist_file,"w"), indent=2)
        print(f"✅ Daily history: {len(daily_hist)} days (all boosted)")
    except Exception as e:
        print(f"⚠️  Daily history error: {e}")
        import traceback; traceback.print_exc()

    # ── Update starting values ──
    starting["monthly"] = monthly
    starting["lifetime"] = lifetime
    starting["last_updated"] = now.strftime("%Y-%m-%d")
    starting["last_run_date"] = today_str
    starting["last_daily"] = {field: daily_data.get(field, 0.0) for field in ADDITIVE_FIELDS}
    prev_today_date = starting.get("previous_today_date", "")
    if prev_today_date and prev_today_date != today_str:
        starting["yesterday"] = starting.get("previous_today", {})
        starting["yesterday_date"] = prev_today_date
    starting["previous_today"] = daily_data
    starting["previous_today_date"] = today_str
    json.dump(starting, open(starting_file,"w"), indent=2)

    print(f"✅ Processing complete (DEMO MODE)!")
    print(f"   PV boosted +{(PV_BOOST-1)*100:.0f}% | Load reduced -{(1-LOAD_REDUCTION)*100:.0f}%")


if __name__ == "__main__":
    main()
