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
    
    # Step 2: Calculate date difference only for rows where date_column > segment_start_column
    df = df.filter(F.col(date_column) > F.col(segment_start_column))
    
    # Calculate date_diff with null handling
    df = df.withColumn("date_diff", F.datediff(F.coalesce(F.col(date_column), F.lit(0)), 
                                               F.coalesce(F.col(segment_start_column), F.lit(0))))
    
    # Step 3: Define window specification to find minimum date_diff within each business_partner group
    window_spec = Window.partitionBy(business_partner_column)
    
    # Step 4: Filter to keep only rows with the minimum `date_diff` in each group
    min_date_diff = F.min("date_diff").over(window_spec)
    closest_df = df.filter(F.col("date_diff") == min_date_diff).drop("date_diff")
    
    # Step 5: Drop duplicates if any (optional safeguard)
    closest_df = closest_df.dropDuplicates()
    
    return closest_df
