import argparse
import os
import pandas as pd
from datetime import datetime
from influxdb_client import InfluxDBClient

# InfluxDB connection settings
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "olAU_9B2denQLHcEwcdUAYWmMmJB3VPOYjTWxIn9C59bz9wkyXgkEw2Ii8OFZxQopnTc_HyZpfu_RumyZCFTJg=="
INFLUXDB_ORG = "CPT"
INFLUXDB_BUCKET = "norden_metrics"

# Reports directory
REPORTS_DIR = r"E:\Norden\Reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

# InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)


def fetch_alarm_data():
    query_api = client.query_api()

    query = f"""
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -12h)
      |> filter(fn: (r) => r._measurement == "machine_performance")
      |> filter(fn: (r) => r._field == "alarm1_id9" or r._field == "alarm1_msg9")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> keep(columns: ["_time", "alarm1_id9", "alarm1_msg9"])
      |> sort(columns: ["_time"], desc: true)
    """

    df = query_api.query_data_frame(query, org=INFLUXDB_ORG)

    if not df.empty:
        df.rename(
            columns={
                "_time": "Time",
                "alarm1_id9": "Alarm ID",
                "alarm1_msg9": "Alarm Message",
            },
            inplace=True,
        )

        df = df.loc[:, ["Time", "Alarm ID", "Alarm Message"]]

        df["Time"] = pd.to_datetime(df["Time"]).dt.tz_localize(None)
        df = df.sort_values("Time", ascending=False).reset_index(drop=True)

        # If Alarm ID = 0, set Alarm Message = "No Alarm"
        df.loc[df["Alarm ID"] == 0, "Alarm Message"] = "No Alarm"

        # Add Duration = previous row's Time - current row's Time (as timedelta)
        df["Duration"] = df["Time"].shift(1) - df["Time"]

    return df


def summarize_alarms(alarms_df):
    if alarms_df.empty:
        return pd.DataFrame(columns=["SN", "Alarm ID", "Alarm Message", "Number of Occurrence", "Total Duration"])

    # Group and aggregate: count + sum
    summary = (
        alarms_df.groupby(["Alarm ID", "Alarm Message"], dropna=False)
        .agg(
            Number_of_Occurrence=("Alarm ID", "count"),
            Total_Duration=("Duration", "sum")
        )
        .reset_index()
    )

    # Add SN column
    summary.insert(0, "SN", range(1, len(summary) + 1))

    # Add TOTAL row
    total_occ = summary["Number_of_Occurrence"].sum()
    total_duration = alarms_df["Duration"].fillna(pd.Timedelta(0)).sum()

    total_row = pd.DataFrame([{
        "SN": "TOTAL",
        "Alarm ID": "",
        "Alarm Message": "",
        "Number of Occurrence": total_occ,
        "Total Duration": total_duration
    }])

    summary = pd.concat([summary, total_row], ignore_index=True)
    return summary


def fetch_performance_data():
    query_api = client.query_api()

    fields = {
        "availability": "Availability",
        "performance": "Performance",
        "quality": "Quality",
        "overalloee": "Overall OEE",
        "machDesignSpeed": "Design Speed",
        "machSpeed": "Set Speed",
        "currMachSpeed": "Current Speed",
        "totalProduced": "Total Produced",
        "totalGoodOut": "Good",
        "badOut": "Bad",
        "dt_NoFault": "Stopped without fault",
        "dt_MachineFault": "Faulty",
        "material_Starved": "No material",
        "upStream_blocked": "Blocked Upstream",
        "downStream_blocked": "Blocked Downstream",
    }

    results = {}

    for field, label in fields.items():
        query = f"""
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: -12h)
          |> filter(fn: (r) => r._measurement == "machine_performance")
          |> filter(fn: (r) => r._field == "{field}")
          |> last()
        """
        tables = query_api.query(query, org=INFLUXDB_ORG)
        value = None
        if tables:
            for table in tables:
                for record in table.records:
                    value = record.get_value()
        results[label] = value

    df = pd.DataFrame(list(results.items()), columns=["Metric", "Value"])
    return df


def generate_excel_report():
    alarms_df = fetch_alarm_data()
    perf_df = fetch_performance_data()
    alarms_summary = summarize_alarms(alarms_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{timestamp}_Norden-Cartoner_report.xlsx"
    filepath = os.path.join(REPORTS_DIR, filename)

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        # Write Alarms sheet
        if not alarms_df.empty:
            alarms_df.to_excel(writer, sheet_name="Alarms", index=False)
            ws_alarms = writer.sheets["Alarms"]

            # Format Duration column
            dur_col = alarms_df.columns.get_loc("Duration") + 1
            for row in range(2, len(alarms_df) + 2):
                val = alarms_df.loc[row - 2, "Duration"]
                if pd.notnull(val):
                    cell = ws_alarms.cell(row=row, column=dur_col, value=val.total_seconds() / (24*3600))
                    cell.number_format = "hh:mm:ss"

        # Write Performance sheet
        if not perf_df.empty:
            perf_df.to_excel(writer, sheet_name="Performance", index=False)
            ws = writer.sheets["Performance"]

            # Format performance metrics
            for row in range(2, len(perf_df) + 2):
                metric = ws[f"A{row}"].value
                cell = ws[f"B{row}"]
                if metric in ["Availability", "Performance", "Quality", "Overall OEE"]:
                    if cell.value is not None:
                        cell.number_format = "0.0%"
                        cell.value = cell.value / 100
                else:
                    cell.number_format = "#,##0"

            # Write alarm summary beside Performance
            if not alarms_summary.empty:
                for col_idx, col_name in enumerate(alarms_summary.columns, start=5):  # E=5
                    ws.cell(row=1, column=col_idx, value=col_name)

                for row_idx, row in enumerate(alarms_summary.itertuples(index=False), start=2):
                    for col_idx, value in enumerate(row, start=5):
                        cell = ws.cell(row=row_idx, column=col_idx)
                        if isinstance(value, pd.Timedelta):
                            cell.value = value.total_seconds() / (24*3600)
                            cell.number_format = "hh:mm:ss"
                        else:
                            cell.value = value

    print(f"Report saved: {filepath}")


def test_connection():
    try:
        query_api = client.query_api()
        test_query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "machine_performance")
          |> filter(fn: (r) => r._field == "alarm1_id9" or r._field == "alarm1_msg9")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> keep(columns: ["_time", "alarm1_id9", "alarm1_msg9"])
          |> limit(n:5)
        '''
        df = query_api.query_data_frame(test_query, org=INFLUXDB_ORG)

        if not df.empty:
            df.rename(
                columns={
                    "_time": "Time",
                    "alarm1_id9": "Alarm ID",
                    "alarm1_msg9": "Alarm Message",
                },
                inplace=True,
            )
            df["Time"] = pd.to_datetime(df["Time"]).dt.tz_localize(None)

        print("[Connection Test] Rows fetched:", len(df))
    except Exception as e:
        print("[Connection Test] Failed:", e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-now", action="store_true", help="Run report immediately")
    args = parser.parse_args()

    if args.run_now:
        test_connection()
        print(f"[{datetime.now()}] Generating report...")
        generate_excel_report()


if __name__ == "__main__":
    main()
