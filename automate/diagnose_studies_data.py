import pandas as pd
import os

self_dir = os.path.dirname(os.path.realpath(__file__))
csv_file_path = os.path.join(self_dir, 'studies_data.csv')

# Load the CSV file into a DataFrame
studies_df = pd.read_csv(csv_file_path)

# Group by 'StudyID' and count occurrences
study_counts = studies_df['StudyID'].value_counts().reset_index()
study_counts.columns = ['StudyID', 'Count']

# Display the counts
print(study_counts)

# Optionally save the counts to a new CSV file
counts_csv_file_path = os.path.join(self_dir, 'study_counts.csv')
study_counts.to_csv(counts_csv_file_path, index=False)
