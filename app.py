# accelerator to parse schedules to find paginated reports

import streamlit as st
import pandas as pd
import io

def process_data(df):
    def generate_report(row):
        if row['Type'] == 'Schedule':
            return row['Name']
        elif row['Type'] == 'JobStepDefinition':
            return row['Name'].split('-')[-1].strip()
        elif row['Type'] == 'JobDefinition':
            return ''
        else:
            return ''

    df['Report'] = df.apply(generate_report, axis=1)
    return df

def extract_job_name(row):
    if row['Type'] == 'Schedule':
        return ''  # No job name in Schedule
    elif row['Type'] == 'JobStepDefinition':
        return row['Name'].split('-')[0].strip()
    elif row['Type'] == 'JobDefinition':
        return row['Name']
    else:
        return ''

def aggregate_data(df):
    # Filter out rows with blank reports (which are from JobDefinition type)
    df_filtered = df[df['Report'] != '']
    
    # Extract job names
    df_filtered['Job'] = df_filtered.apply(extract_job_name, axis=1)
    
    # Convert all Recipient values to strings to avoid issues with non-string types
    df_filtered['Recipient'] = df_filtered['Recipient'].astype(str)
    
    # Group by Report and aggregate the recipients and job names
    df_aggregated = df_filtered.groupby('Report').agg(
        schedule_recipients=pd.NamedAgg(column='Recipient', aggfunc=lambda x: ', '.join(x[df_filtered['Type'] == 'Schedule'])),
        jobstepdefinition_recipients=pd.NamedAgg(column='Recipient', aggfunc=lambda x: ', '.join(x[df_filtered['Type'] == 'JobStepDefinition'])),
        jobs=pd.NamedAgg(column='Job', aggfunc=lambda x: ', '.join(x.dropna().unique()))
    ).reset_index()

    return df_aggregated

st.title('Report Column Generator & Aggregator')

st.write('Upload a CSV file with columns: Name, Type, Recipient')

uploaded_file = st.file_uploader("Choose a file", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    
    if set(['Name', 'Type', 'Recipient']).issubset(df.columns):
        st.write("Original Data:")
        st.write(df)

        processed_df = process_data(df)

        st.write("Processed Data:")
        st.write(processed_df)

        aggregated_df = aggregate_data(processed_df)

        st.write("Aggregated Data by Report:")
        st.write(aggregated_df)

        output = io.StringIO()
        aggregated_df.to_csv(output, index=False)
        st.download_button(
            label="Download Aggregated Data as CSV",
            data=output.getvalue(),
            file_name="aggregated_data.csv",
            mime="text/csv"
        )
    else:
        st.error('The uploaded file does not have the required columns.')
