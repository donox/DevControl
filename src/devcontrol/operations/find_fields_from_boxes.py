import pandas as pd


def process_groups(input_file, output_file):
    # Read CSV file
    df = pd.read_csv(input_file)

    # Group by the 'group' column and aggregate
    grouped = df[df['group'].notna()].groupby('group').agg({
        'top': 'min',
        'left': 'min',
        'bottom': 'max',
        'right': 'max'
    }).reset_index()

    # Write results to CSV
    grouped.to_csv(output_file, index=False)


# Usage
process_groups('input.csv', 'output.csv')