"""
Generate sample data using pandas for visualization
This script creates sample time-series data and exports it as JSON
"""
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_line_graph_data():
    """
    Generate sample data for a line graph
    Returns data in JSON format suitable for Chart.js or similar libraries
    """
    # Generate dates for the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=29)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Generate sample data using numpy
    np.random.seed(42)
    
    # Create a DataFrame with sample financial data
    df = pd.DataFrame({
        'date': dates,
        'revenue': np.random.randint(10000, 50000, size=len(dates)) + np.linspace(10000, 20000, len(dates)),
        'expenses': np.random.randint(5000, 30000, size=len(dates)) + np.linspace(5000, 15000, len(dates)),
        'profit': 0  # Will calculate this
    })
    
    # Calculate profit
    df['profit'] = df['revenue'] - df['expenses']
    
    # Add some statistical analysis
    summary_stats = {
        'revenue': {
            'mean': float(df['revenue'].mean()),
            'median': float(df['revenue'].median()),
            'std': float(df['revenue'].std()),
            'min': float(df['revenue'].min()),
            'max': float(df['revenue'].max())
        },
        'expenses': {
            'mean': float(df['expenses'].mean()),
            'median': float(df['expenses'].median()),
            'std': float(df['expenses'].std()),
            'min': float(df['expenses'].min()),
            'max': float(df['expenses'].max())
        },
        'profit': {
            'mean': float(df['profit'].mean()),
            'median': float(df['profit'].median()),
            'std': float(df['profit'].std()),
            'min': float(df['profit'].min()),
            'max': float(df['profit'].max())
        }
    }
    
    # Convert to JSON-friendly format
    data = {
        'labels': df['date'].dt.strftime('%Y-%m-%d').tolist(),
        'datasets': [
            {
                'label': 'Revenue',
                'data': df['revenue'].round(2).tolist(),
                'borderColor': 'rgb(75, 192, 192)',
                'backgroundColor': 'rgba(75, 192, 192, 0.2)',
                'tension': 0.4
            },
            {
                'label': 'Expenses',
                'data': df['expenses'].round(2).tolist(),
                'borderColor': 'rgb(255, 99, 132)',
                'backgroundColor': 'rgba(255, 99, 132, 0.2)',
                'tension': 0.4
            },
            {
                'label': 'Profit',
                'data': df['profit'].round(2).tolist(),
                'borderColor': 'rgb(54, 162, 235)',
                'backgroundColor': 'rgba(54, 162, 235, 0.2)',
                'tension': 0.4
            }
        ],
        'statistics': summary_stats
    }
    
    return data

def main():
    """Generate and save sample data"""
    data = generate_line_graph_data()
    
    # Save to JSON file
    with open('public/data/sample_data.json', 'w') as f:
        json.dump(data, f, indent=2)
    
    print("Sample data generated successfully!")
    print(f"Total data points per series: {len(data['labels'])}")
    print("\nStatistics Summary:")
    for metric, stats in data['statistics'].items():
        print(f"\n{metric.upper()}:")
        print(f"  Mean: ${stats['mean']:.2f}")
        print(f"  Min: ${stats['min']:.2f}")
        print(f"  Max: ${stats['max']:.2f}")

if __name__ == '__main__':
    main()
