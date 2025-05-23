import pandas as pd
import streamlit as st
from datetime import datetime
import os
from math import radians, sin, cos, sqrt, atan2
from fuzzywuzzy import fuzz, process

OUTPUT_FOLDER = "Output"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
DEFAULT_TIME_RANGE = (3, 45)
DEFAULT_DISTANCE_THRESHOLD = 150
FUZZY_MATCH_THRESHOLD = 90

def clean_customer_name(name):
    return str(name).lower().replace('d.o.o.', '').replace('d.d.', '').replace(',', '').strip()

def fuzzy_match_customers(df):
    unique_names = df['Customer Name'].fillna('').unique()
    clusters = {}
    for name in unique_names:
        cleaned = clean_customer_name(name)
        if not clusters:
            clusters[cleaned] = [name]
            continue
        match_result = process.extractOne(cleaned, clusters.keys(), scorer=fuzz.token_set_ratio)
        if match_result is not None:
            match, score = match_result
            if score >= FUZZY_MATCH_THRESHOLD:
                clusters[match].append(name)
            else:
                clusters[cleaned] = [name]
        else:
            clusters[cleaned] = [name]
    name_map = {}
    for cluster, members in clusters.items():
        representative = max(members, key=len)
        for member in members:
            name_map[member] = representative
    return name_map

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(radians, map(float, [lat1, lon1, lat2, lon2]))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a)) * 1000

def load_data(uploaded_file):
    try:
        df = pd.read_excel(
            uploaded_file,
            engine='openpyxl',
            parse_dates={'Act_Datetime': ['Act Dt', 'Act Tm']},
            dtype={
                'PUD Rte': str,
                'Customer Name': str,
                'awb_booking': str,
                'Total Pcs': float,
                'lat': float,
                'lgtd': float,
                'Act Ckpt Code': str
            }
        )
        df.columns = [str(col) for col in df.columns]
        required_columns = [
            'PUD Rte', 'Customer Name', 'Act_Datetime',
            'lat', 'lgtd', 'awb_booking', 'Total Pcs', 'Act Ckpt Code'
        ]
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            st.error(f"Missing required columns: {', '.join(missing)}")
            return None
        df['Customer Name'] = df['Customer Name'].fillna('UNKNOWN')
        name_map = fuzzy_match_customers(df)
        df['Customer_Cluster'] = df['Customer Name'].map(name_map)
        df['Total Pcs'] = pd.to_numeric(df['Total Pcs'], errors='coerce').fillna(0).astype(int)
        return df.dropna(subset=required_columns)
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return None

def detect_violations(df, min_time, max_time, distance_threshold):
    records = []
    df['Date'] = df['Act_Datetime'].dt.date
    grouped = df.groupby(['Route', 'Customer_Cluster', 'Date'])
    for (route, customer, date), group in grouped:
        group = group.sort_values('Act_Datetime')
        if len(group) < 2:
            continue
        for checkpoint_type in ['OK', 'PU']:
            type_group = group[group['Act Ckpt Code'] == checkpoint_type]
            if len(type_group) == 0:
                continue
            first_scan = type_group.iloc[0]
            for idx, row in type_group.iterrows():
                is_first = (idx == type_group.index[0])
                time_diff = (row['Act_Datetime'] - first_scan['Act_Datetime']).total_seconds() / 60
                distance = haversine(
                    first_scan['lat'], first_scan['lgtd'],
                    row['lat'], row['lgtd']
                )
                reasons = []
                is_violation = False
                if not is_first:
                    # Time violation only if within selected window
                    if min_time <= time_diff <= max_time:
                        reasons.append(f"Time {round(time_diff,2)}min in [{min_time}-{max_time}]min")
                        is_violation = True
                    # Distance violation (always triggers regardless of time)
                    if distance > distance_threshold:
                        reasons.append(f"Distance > {distance_threshold}m")
                        is_violation = True
                records.append({
                    'Date': date,
                    'Route': route,
                    'Original_Customer': row['Customer Name'],
                    'AWB': row['awb_booking'],
                    'Total Pcs': row['Total Pcs'],
                    'Act Time': row['Act_Datetime'].strftime('%H:%M:%S'),
                    'Checkpoint Type': checkpoint_type,
                    'Time_From_First_Min': round(time_diff, 2),
                    'Distance_From_First_m': round(distance, 2),
                    'Latitude': row['lat'],
                    'Longitude': row['lgtd'],
                    'Violation Reasons': "; ".join(reasons),
                    'Is_Violation': is_violation
                })
    return pd.DataFrame(records)

def generate_report(violations_df, all_events_df):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(OUTPUT_FOLDER, f"compliance_report_{timestamp}.xlsx")
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Summary
        summary = violations_df[violations_df['Is_Violation']].groupby('Route').agg(
            Total_Violations=('AWB', 'nunique'),
            Unique_Customers=('Original_Customer', 'nunique')
        ).reset_index()
        summary.to_excel(writer, sheet_name='Summary', index=False)

        # Triggered Shipments (all events including non-violations)
        triggered_cols = [
            'Date', 'Route', 'Original_Customer', 'AWB',
            'Total Pcs', 'Act Time', 'Checkpoint Type', 'Time_From_First_Min',
            'Distance_From_First_m', 'Latitude', 'Longitude', 'Violation Reasons', 'Is_Violation'
        ]
        violations_df[triggered_cols].to_excel(writer, sheet_name='Triggered Shipments', index=False)

        # Violations (only actual violations)
        violations_only = violations_df[violations_df['Is_Violation']]
        violations_only[triggered_cols].to_excel(writer, sheet_name='Violations', index=False)

        # All Checkpoints (transparency)
        transparency_cols = [
            'Date', 'Route', 'Original_Customer', 'AWB',
            'Total Pcs', 'Act Time', 'Checkpoint Type', 'Latitude', 'Longitude'
        ]
        all_events_df[transparency_cols].to_excel(writer, sheet_name='All_Checkpoints', index=False)
    return output_path

def main():
    st.set_page_config(
        page_title="Delivery Compliance Monitor",
        page_icon="🚚",
        layout="wide"
    )
    st.title("🚚 Courier Compliance Monitoring System")

    # Range slider for time threshold
    min_time, max_time = st.sidebar.slider(
        "⏱️ Time threshold range (minutes)",
        min_value=1,
        max_value=360,
        value=DEFAULT_TIME_RANGE,
        step=1,
        help="Set the minimum and maximum time (in minutes) between scans to flag as violation."
    )

    distance_threshold = st.sidebar.slider(
        "📏 Distance threshold (meters)",
        50, 1000, DEFAULT_DISTANCE_THRESHOLD
    )

    uploaded_file = st.file_uploader(
        "Upload Daily Report",
        type=["xlsm", "xlsx"],
        help="Supports .xlsm (macro-enabled) and .xlsx formats"
    )

    if uploaded_file:
        df = load_data(uploaded_file)
        if df is None:
            return

        # Rename 'PUD Rte' to 'Route' in the original DataFrame
        df = df.rename(columns={'PUD Rte': 'Route'})

        # Prepare all_events_df
        all_events_df = df.copy()
        all_events_df = all_events_df.rename(columns={
            'awb_booking': 'AWB',
            'Act Ckpt Code': 'Checkpoint Type',
            'lat': 'Latitude',
            'lgtd': 'Longitude'
        })
        all_events_df['Date'] = all_events_df['Act_Datetime'].dt.date
        all_events_df['Act Time'] = all_events_df['Act_Datetime'].dt.strftime('%H:%M:%S')
        all_events_df['Original_Customer'] = all_events_df['Customer Name']

        with st.expander("Preview Uploaded Data", expanded=False):
            st.dataframe(df.head(3))

        if st.button("Analyze Deliveries"):
            with st.spinner("Processing data..."):
                try:
                    violations_df = detect_violations(df, min_time, max_time, distance_threshold)
                    group_sizes = df.groupby(['Route', 'Customer_Cluster', 'Date']).size()
                    multi_groups = group_sizes[group_sizes > 1].reset_index()[['Route', 'Customer_Cluster', 'Date']]
                    all_events_df_multi = pd.merge(
                        all_events_df,
                        multi_groups,
                        how='inner',
                        left_on=['Route', 'Customer_Cluster', 'Date'],
                        right_on=['Route', 'Customer_Cluster', 'Date']
                    )
                    if not violations_df.empty:
                        report_path = generate_report(violations_df, all_events_df_multi)
                        with open(report_path, "rb") as f:
                            st.download_button(
                                label="📥 Download Full Report",
                                data=f,
                                file_name=os.path.basename(report_path),
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        st.subheader("Analysis Results")
                        col1, col2, col3 = st.columns(3)
                        valid_violations = violations_df[violations_df['Is_Violation']]
                        col1.metric("Total Violations", valid_violations['AWB'].nunique())
                        col2.metric("Affected Customers", valid_violations['Original_Customer'].nunique())
                        col3.metric("Affected Routes", valid_violations['Route'].nunique())
                        st.dataframe(
                            valid_violations[
                                ['Date', 'Route', 'Original_Customer', 'AWB',
                                 'Checkpoint Type', 'Violation Reasons']
                            ].head(10)
                        )
                    else:
                        st.success("✅ No compliance violations detected")
                except ImportError:
                    st.error("Required packages missing! Install with:")
                    st.code("pip install fuzzywuzzy python-Levenshtein")

if __name__ == "__main__":
    main()
