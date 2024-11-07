Test Set Classification Report:
              precision    recall  f1-score   support

           0       0.04      0.93      0.07       858
           1       0.98      0.14      0.25     25416

    accuracy                           0.17     26274
    macro avg       0.51      0.54      0.16     26274
    weighted avg       0.95      0.17      0.24     26274

Rolling day metrics
Test Set Classification Report:
              precision    recall  f1-score   support

           0       0.66      0.98      0.79     17280
           1       0.34      0.02      0.04      8994

    accuracy                           0.65     26274
    macro avg       0.50      0.50      0.41     26274
    weighted avg       0.55      0.65      0.53     26274

group2_df = spark.sql("""
SELECT spp_result.BUSINESS_PARTN, spp_result.START_BB_PERIOD, spp_result.END_BB_PERIOD, dashboard.*
FROM (
    SELECT DISTINCT BUSINESS_PARTN, START_BB_PERIOD, END_BB_PERIOD
    FROM prod.cds_cods.eabp_v AS a
    WHERE a.PYMT_PLAN_TYPE='spp'
      AND a.START_BB_PERIOD <= '2024-08-29'
      AND a.END_BB_PERIOD > '2024-08-19'
) AS spp
LEFT ANTI JOIN dev.uncertified.week44_spp_weeklyreminder AS pilot
ON spp.BUSINESS_PARTN = pilot.`Business Partner`
) AS spp_result
INNER JOIN common.call_volume_dashboard AS dashboard
ON spp_result.BUSINESS_PARTN = dashboard.BUSINESS_PARTNER_ID
""")

