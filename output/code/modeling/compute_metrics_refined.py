import pandas as pd


def compute_utilization_trends(df_station: pd.DataFrame) -> pd.DataFrame:
    r"""
    Compute daily average occupancy rate per station.

    Args:
        df_station (pd.DataFrame): DataFrame with columns ['station_id', 'hour', 'occupancy_rate']

    Returns:
        pd.DataFrame: DataFrame with columns ['station_id', 'date', 'avg_occupancy_rate']
    """
    df = df_station.copy()
    df['date'] = df['hour'].dt.date
    # Compute mean occupancy_rate per day
    daily = df.groupby(['station_id', 'date'])['occupancy_rate'].mean().reset_index()
    daily = daily.rename(columns={'occupancy_rate': 'avg_occupancy_rate'})
    return daily
    r"""
    Compute daily average occupancy rate per station.

    Args:
        df_station (pd.DataFrame): DataFrame with columns ['station_id', 'hour', 'occupancy_rate']

    Returns:
        pd.DataFrame: DataFrame with columns ['station_id', 'date', 'avg_occupancy_rate']
    """
    r"""
    Compute daily average occupancy rate per station.

    Args:
        df_station (pd.DataFrame): DataFrame with columns ['station_id', 'hour', 'occupancy_rate']

    Returns:
        pd.DataFrame: DataFrame with columns ['station_id', 'date', 'avg_occupancy_rate']
    """
    df = df_station.copy()
    df['date'] = df['hour'].dt.date
    daily = df.groupby(['station_id', 'date']).agg(
        avg_occupancy_rate=pd.NamedAgg(column='occupancy_rate', aggfunc='mean')
    ).reset_index()
    return daily


def compute_peak_periods(df_station: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    r"""
    Identify top N peak hours per station by occupancy rate.

    Args:
        df_station (pd.DataFrame): DataFrame with columns ['station_id', 'hour', 'occupancy_rate']
        top_n (int): Number of top hours to select.

    Returns:
        pd.DataFrame: DataFrame with ['station_id', 'hour', 'occupancy_rate'], sorted by station_id, occupancy_rate desc.
    """
    return (
        df_station.sort_values(['station_id', 'occupancy_rate'], ascending=[True, False])
        .groupby('station_id')
        .head(top_n)
        .reset_index(drop=True)
    )


def compute_global_peak_by_hour_of_day(df_station: pd.DataFrame) -> pd.DataFrame:
    r"""
    Compute average occupancy rate for each hour-of-day across all stations.

    Args:
        df_station (pd.DataFrame): DataFrame with ['hour', 'occupancy_rate']

    Returns:
        pd.DataFrame: ['hour_of_day', 'avg_occupancy_rate']
    """
    df = df_station.copy()
    df['hour_of_day'] = df['hour'].dt.hour
    hourly = df.groupby('hour_of_day')['occupancy_rate'].mean().reset_index(
        name='avg_occupancy_rate'
    )
    return hourly


def compute_capacity_pressure(df_station: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
    r"""
    Compute capacity pressure per station: hours where occupancy_rate >= threshold.

    Args:
        df_station (pd.DataFrame): DataFrame with ['station_id', 'occupancy_rate']
        threshold (float): Occupancy_rate threshold.

    Returns:
        pd.DataFrame: ['station_id', 'total_hours', 'high_pressure_hours', 'high_pressure_ratio']
    """
    df = df_station.copy()
    agg = df.groupby('station_id').agg(
        total_hours=pd.NamedAgg(column='occupancy_rate', aggfunc='count'),
        high_pressure_hours=pd.NamedAgg(
            column='occupancy_rate', aggfunc=lambda x: (x >= threshold).sum()
        ),
    ).reset_index()
    agg['high_pressure_ratio'] = agg['high_pressure_hours'] / agg['total_hours']
    return agg


def compute_reliability_downtime(df_evse: pd.DataFrame, gap_hours: int = 24) -> pd.DataFrame:
    r"""
    Detect prolonged downtime events per EVSE where charging_count == 0 for >= gap_hours contiguous hours.

    Args:
        df_evse (pd.DataFrame): DataFrame with ['evse_id', 'hour', 'charging_count']
        gap_hours (int): Minimum contiguous hours to consider as downtime.

    Returns:
        pd.DataFrame: ['evse_id', 'num_downtime_events', 'total_downtime_hours']
    """
    results = []
    for evse_id, group in df_evse.groupby('evse_id'):
        grp = group.sort_values('hour')
        grp['is_zero'] = grp['charging_count'] == 0
        grp['run'] = (grp['is_zero'] != grp['is_zero'].shift(1)).cumsum()
        runs = grp.groupby('run').agg(
            evse_id=('evse_id', 'first'),
            is_zero=('is_zero', 'first'),
            count_hours=('hour', 'count')
        )
        downtime_runs = runs[runs['is_zero'] & (runs['count_hours'] >= gap_hours)]
        num_events = downtime_runs.shape[0]
        total_hours = downtime_runs['count_hours'].sum()
        results.append((evse_id, num_events, total_hours))
    df_res = pd.DataFrame(results, columns=['evse_id', 'num_downtime_events', 'total_downtime_hours'])
    return df_res


def main():
    # Load station occupancy time series
    station_df = pd.read_parquet('/root/output/data/occupancy_station_hourly.parquet').reset_index()
    station_df['hour'] = pd.to_datetime(station_df['hour'])

    # Compute refined occupancy_rate: exclude OUTOFORDER and UNKNOWN, denominator = charging + available
    # occupancy_station_hourly excludes UNKNOWN; now exclude OUTOFORDER hours from denominator calculation
    station_df['occupied_msgs'] = station_df['charging']
    station_df['available_msgs'] = station_df['AVAILABLE']
    # Avoid division by zero
    station_df['occupancy_rate_refined'] = station_df['occupied_msgs'] / (station_df['occupied_msgs'] + station_df['available_msgs'])
    station_ref = station_df.dropna(subset=['occupancy_rate_refined'])
    # Remove original occupancy_rate column to avoid duplication
    station_ref = station_ref.drop(columns=['occupancy_rate']).rename(columns={'occupancy_rate_refined': 'occupancy_rate'})

    # Load EVSE occupancy data
    evse_df = pd.read_parquet('/root/output/data/occupancy_evse_hourly.parquet').reset_index()
    evse_df['hour'] = pd.to_datetime(evse_df['hour'])
    evse_df['charging_count'] = evse_df['charging']

    # Compute metrics using refined occupancy_rate where applicable
    daily_util = compute_utilization_trends(station_ref[['station_id', 'hour', 'occupancy_rate']])
    peak_periods = compute_peak_periods(station_ref[['station_id', 'hour', 'occupancy_rate']])
    global_peak = compute_global_peak_by_hour_of_day(station_ref[['hour', 'occupancy_rate']])
    capacity_pressure = compute_capacity_pressure(station_ref[['station_id', 'occupancy_rate']])
    reliability = compute_reliability_downtime(evse_df[['evse_id', 'hour', 'charging_count']])

    # Save refined outputs
    daily_util.to_csv('/root/output/outputs/daily_utilization_trends_refined.csv', index=False)
    peak_periods.to_csv('/root/output/outputs/peak_periods_station_refined.csv', index=False)
    global_peak.to_csv('/root/output/outputs/global_peak_hour_of_day_refined.csv', index=False)
    capacity_pressure.to_csv('/root/output/outputs/capacity_pressure_station_refined.csv', index=False)
    reliability.to_csv('/root/output/outputs/reliability_evse_downtime_refined.csv', index=False)


if __name__ == '__main__':
    main()