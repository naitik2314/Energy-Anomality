# Variables
start_window = pd.to_datetime('2024-08-19')
end_window = pd.to_datetime('2024-09-29')

input_file = "SPP Pilot - Enrollment Data for AHT.xlsx"
sheet_name = "Master List"
output_file = "SPP_CA.csv"
column_name = "Contract Account"
BMD = '//Nas1/CS Analytics/C2A_Prod/BMD.parquet'

# We are using this cell in the main input cell
def business_partner_join(BMD, input_file, output_file, sheet_name):
    df = (pd.read_excel(input_file, sheet_name=sheet_name, engine="openpyxl", dtype={'Contract Account': 'string'})).rename(columns={'Contract Account': 'contract_account'}).merge(
        (pd.read_parquet(BMD, columns=['business_partner', 'contract_account'])).astype(str),
    on = 'contract_account',
    how = 'left')
    
    df.to_csv(output_file)

business_partner_join(BMD, input_file, output_file, sheet_name)

call_volume = (
    pd.read_parquet('//Nas1/CS Analytics/C2A_Prod/FULL_CALL_VOLUME_DASHBOARD.parquet', columns=['segment_start', 'business_partner_id', 'segment_stop', 'vendor', 'username', 'call_id', 'ucid', 'media_id', 'handled_time', 'contract_account', 'prorated_call_new', 'node_l2', 'node_l3'])
    .assign(segment_start = lambda df: pd.to_datetime(df['segment_start']),
            segment_stop = lambda df: pd.to_datetime(df['segment_stop']))
    .rename(columns={'call_id': 'CALL_ID', 'media_id': 'MEDIA_ID', 'business_partner_id': 'business_partner'})
    .query("segment_start >= '2024-08-19' and segment_stop <= '2024-09-29'") #change this to the variables
    .query("node_l2 == 'SPP'")
    .dropna(subset=['contract_account'])
    .drop_duplicates()
)

call_volume['business_partner'] = call_volume['business_partner'].astype(str)
call_volume['contract_account'] = call_volume['contract_account'].astype(str)

spp_pilot_contract_accounts = (
    pd.read_csv(output_file, dtype={'business_partner': 'string', 'contract_account': 'string'}).rename(columns={'business_partner': 'pilot_business_partner', 'contract_account': 'pilot_contract_account'})[['pilot_business_partner', 'pilot_contract_account']].drop_duplicates()
)

spp_pilot = (
    pd.read_csv(output_file, dtype={'business_partner': 'string', 'contract_account': 'string'}).rename(columns={'Opt-In Date': 'pilot_start_date'})[['business_partner', 'pilot_start_date', 'contract_account', 'Plan Created by']].drop_duplicates()
    .assign(
        pilot_start_date=lambda x: pd.to_datetime(x['pilot_start_date']),
    )
    .assign(
        pilot_start_date_only=lambda x: x['pilot_start_date'].dt.date
    )
)

spp_enrolled = (
    pd.read_parquet('//Nas1/CS Analytics/C2A_Prod/PAYMENT_PLANS.parquet', columns=['contract_account', 'payment_plan_type', 'business_partner'])
    .query("payment_plan_type == 'SPP'")
    .merge(
        pd.read_parquet('//Nas1/CS Analytics/C2A_Prod/INSTALLMENT_PLANS.parquet', columns=['deactivation_date', 'plan_start_date', 'plan_end_date', 'contract_account', 'business_partner']), on = ['contract_account', 'business_partner'], how = 'inner'
    )
    .drop_duplicates()
)

# Group 1
spp_pitch_calls_pilot_accepted = spp_pilot.merge(
    call_volume,
    on = 'business_partner',
    how = 'inner'
)

spp_pitch_calls_pilot_accepted = spp_pitch_calls_pilot_accepted.loc[
    spp_pitch_calls_pilot_accepted['pilot_start_date'] >= spp_pitch_calls_pilot_accepted['segment_start']
].drop_duplicates()

# Group 2
spp_enrolled_not_pilot = spp_enrolled.assign(
    effective_end_date = spp_enrolled['deactivation_date'].fillna(spp_enrolled['plan_end_date'])
).query(
    "(plan_start_date <= @end_window) & (effective_end_date >= @start_window)"
).loc[
    ~spp_enrolled['business_partner'].isin(spp_pilot_contract_accounts['pilot_business_partner'])
].drop(
    columns = ['deactivation_date', 'plan_end_date', 'effective_end_date', 'payment_plan_type']
)

spp_pitch_calls = spp_enrolled_not_pilot.merge(
    call_volume,
    on = 'business_partner',
    how = 'inner'
)

spp_pitch_calls = spp_pitch_calls.loc[
    spp_pitch_calls['plan_start_date'] >= spp_pitch_calls['segment_start']
].drop_duplicates()

# Group 3
spp_combined_accounts = pd.concat([
    spp_pilot['business_partner'], 
    spp_enrolled_not_pilot['business_partner']
]).drop_duplicates()

accounts_not_in_spp = call_volume[
    ~call_volume['business_partner'].isin(spp_combined_accounts)
]
accounts_not_in_spp = accounts_not_in_spp.drop_duplicates()
print("Business Partner present in call volume but not in SPP Pilot or SPP Enrolled stored")

def check_and_filter_by_closest_date(df, business_partner_column, date_column, segment_start_column, node_column):
    if df[business_partner_column].nunique() == df.shape[0]:
        print("Good to go: All values in the specified column are unique.")
    else:
        print("Warning: Some values are duplicated in the specified column.")
    df['date_diff'] = (df[date_column] - df[segment_start_column])
    
    def get_closest_row(group):
        if len(group) == 1:
            return group.iloc[0]
        
        spp_enroll_rows = group[group[node_column] == 'SPP ENROLL']
        
        if len(spp_enroll_rows) == len(group):
            return spp_enroll_rows.loc[spp_enroll_rows['date_diff'].idxmin()]
        
        if not spp_enroll_rows.empty:
            return spp_enroll_rows.loc[spp_enroll_rows['date_diff'].idxmin()]
        
        return group.loc[group['date_diff'].idxmin()]
    
    closest_df = df.groupby(business_partner_column).apply(get_closest_row).reset_index(drop=True)
    closest_df = closest_df.drop(columns=['date_diff'])
    closest_df = closest_df.drop_duplicates()
    
    return closest_df

def calculate_average_handle_time(df, handle_time_column):
    total_handle_time = df[handle_time_column].sum()
    total_prorated = df['prorated_call_new'].sum()
    average_handle_time = total_handle_time / total_prorated 
    return total_handle_time, average_handle_time

# df_filtered = check_and_filter_by_closest_date(call_volume, 'contract_account', 'plan_start_date', 'segment_start')
# avg_handle_time = calculate_average_handle_time(call_volume, 'handle_time')

# Group 1 value calculation
group1_filtered = check_and_filter_by_closest_date(spp_pitch_calls_pilot_accepted, 'business_partner', 'pilot_start_date', 'segment_start', 'node_l3')
group1_handle_time, group1_avg_handle_time = calculate_average_handle_time(group1_filtered, 'handled_time')
print(f"Total handle time: {group1_handle_time} and Total number of calls: {group1_filtered.shape[0]} and Avg Handle Time: {group1_avg_handle_time}")

# Group 2 value calculation
group2_filtered = check_and_filter_by_closest_date(spp_pitch_calls, 'business_partner', 'plan_start_date', 'segment_start', 'node_l3')
group2_handle_time, group2_avg_handle_time = calculate_average_handle_time(group2_filtered, 'handled_time')
print(f"Total handle time: {group2_handle_time} and Total number of calls: {group2_filtered.shape[0]} and Avg Handle Time: {group2_avg_handle_time}")

# Group 3 value calculation
accounts_not_in_spp = accounts_not_in_spp[accounts_not_in_spp['node_l3'] == 'SPP ENROLL']
group3_handle_time, group3_avg_handle_time = calculate_average_handle_time(accounts_not_in_spp, 'handled_time')
print(f"Total handle time: {group3_handle_time} and Total number of calls: {accounts_not_in_spp.shape[0]} and Avg Handle Time: {group3_avg_handle_time}")
