import os
import glob
import pandas as pd


class DataMerger:
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = input_dir
        self.output_dir = output_dir

    def merge(self):
        print(f"Looking for CSV files in '{self.input_dir}'...")
        # Recursively find all .csv files in the input directory
        search_pattern = os.path.join(self.input_dir, '**', '*.csv')
        csv_files = glob.glob(search_pattern, recursive=True)

        if not csv_files:
            print("No CSV files found to merge!")
            return

        print(f"Found {len(csv_files)} CSV files. Merging...")

        df_list = []
        for file in csv_files:
            print(f"  -> Reading {file}...")
            try:
                df = pd.read_csv(file)
                df_list.append(df)
            except Exception as e:
                print(f"  -> Error reading {file}: {e}")

        if not df_list:
            print("No valid data could be read.")
            return

        # Combine all files into one large DataFrame
        master_df = pd.concat(df_list, ignore_index=True)

        # Remove duplicates
        initial_count = len(master_df)

        # If your data has a 'url' or 'id' column, it's safer to drop duplicates based on that.
        # Otherwise, drop rows that are exactly identical.
        if 'url' in master_df.columns:
            master_df = master_df.drop_duplicates(subset=['url'])
        elif 'id' in master_df.columns:
            master_df = master_df.drop_duplicates(subset=['id'])
        else:
            master_df = master_df.drop_duplicates()

        print(f"Merged {initial_count} rows. After removing duplicates: {len(master_df)} unique listings.")

        # Save the final files
        csv_output = os.path.join(self.output_dir, 'master_listings.csv')
        xlsx_output = os.path.join(self.output_dir, 'master_listings.xlsx')

        print(f"Saving to {csv_output}...")
        master_df.to_csv(csv_output, index=False, sep=';', encoding='utf-8-sig')

        print(f"Saving to {xlsx_output}...")
        master_df.to_excel(xlsx_output, index=False)

        print("Merge complete!")