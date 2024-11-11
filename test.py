from pyspark.sql import functions as F
from pyspark.sql.window import Window

def check_and_filter_by_closest_date(df, business_partner_column, date_column, segment_start_column):
    # Step 1: Check uniqueness of the business partner column
    unique_count = df.select(business_partner_column).distinct().count()
    total_count = df.count()
    
    if unique_count == total_count:
        print("Good to go: All values in the specified column are unique.")
    else:
        print("Warning: Some values are duplicated in the specified column.")
    
    # Step 2: Filter rows where date_column > segment_start_column
    df = df.filter(F.col(date_column) > F.col(segment_start_column))
    
    # Calculate date difference
    df = df.withColumn("date_diff", F.datediff(F.col(date_column), F.col(segment_start_column)))
    
    # Step 3: Define window specification to find minimum date_diff within each business_partner group
    window_spec = Window.partitionBy(business_partner_column)
    
    # Step 4: Add a column with the minimum date_diff within each group
    df = df.withColumn("min_date_diff", F.min("date_diff").over(window_spec))
    
    # Step 5: Filter to keep only rows where date_diff is equal to min_date_diff in each group
    closest_df = df.filter(F.col("date_diff") == F.col("min_date_diff")).drop("date_diff", "min_date_diff")
    
    # Step 6: Drop duplicates if any (optional safeguard)
    closest_df = closest_df.dropDuplicates()
    
    return closest_df
