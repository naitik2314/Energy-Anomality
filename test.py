from pyspark.sql import functions as F
from pyspark.sql.window import Window

def check_and_filter_by_closest_date(df, business_partner_column, date_column, segment_start_column, node_column):
    # Check uniqueness of the business partner column
    unique_count = df.select(business_partner_column).distinct().count()
    total_count = df.count()
    
    if unique_count == total_count:
        print("Good to go: All values in the specified column are unique.")
    else:
        print("Warning: Some values are duplicated in the specified column.")
    
    # Calculate date difference
    df = df.withColumn("date_diff", F.datediff(F.col(date_column), F.col(segment_start_column)))
    
    # Define window to find the closest date difference within each group
    window_spec = Window.partitionBy(business_partner_column)
    
    # Find the minimum date_diff for rows with "SPP ENROLL" in node_column
    df = df.withColumn("spp_enroll_min_date_diff", 
                       F.when(F.col(node_column) == "SPP ENROLL", F.col("date_diff")).otherwise(F.lit(None)))
    df = df.withColumn("spp_enroll_min_date_diff", F.min("spp_enroll_min_date_diff").over(window_spec))
    
    # Select rows based on the closest `date_diff` rule
    df = df.withColumn("selected_row",
                       F.when((F.col(node_column) == "SPP ENROLL") & (F.col("date_diff") == F.col("spp_enroll_min_date_diff")),
                              1)
                       .when(F.col("date_diff") == F.min("date_diff").over(window_spec), 1)
                       .otherwise(0))
    
    # Filter only the selected rows
    closest_df = df.filter(F.col("selected_row") == 1).drop("date_diff", "spp_enroll_min_date_diff", "selected_row")
    
    # Drop duplicates, if any
    closest_df = closest_df.dropDuplicates()
    
    return closest_df
