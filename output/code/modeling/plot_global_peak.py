import pandas as pd
import matplotlib.pyplot as plt
import time


def plot_global_peak():
    r"""
    Generate a bar plot of average occupancy rate by hour of day across all stations.

    Reads the file '/root/output/outputs/global_peak_hour_of_day.csv',
    and saves the plot to '/root/output/outputs/global_peak_1_<timestamp>.png'.
    """
    # Load data
    df = pd.read_csv('/root/output/outputs/global_peak_hour_of_day.csv')

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(df['hour_of_day'], df['avg_occupancy_rate'] * 100, color='skyblue')
    ax.set_xlabel('Hour of Day (0-23)')
    ax.set_ylabel('Average Occupancy Rate (%)')
    ax.set_title('Average Station Occupancy Rate by Hour of Day')
    ax.set_xticks(range(0, 24))
    ax.relim()
    ax.autoscale_view()

    # Save plot
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    filename = f'/root/output/outputs/global_peak_1_{timestamp}.png'
    fig.savefig(filename, dpi=300)
    plt.close(fig)
    print(f"Plot saved to {filename}")


if __name__ == '__main__':
    plot_global_peak()