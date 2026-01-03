import pandas as pd

# --------------------------------------------------
# STEP 1: Read employee data from CSV
# --------------------------------------------------
df = pd.read_csv("data/employees.csv")

print("Original Data:")
print(df)

# --------------------------------------------------
# STEP 2: Handle missing salary values
# --------------------------------------------------
df["salary"] = df.groupby("department")["salary"].transform(
    lambda x: x.fillna(x.mean())
)

overall_mean_salary = df["salary"].mean()
df["salary"] = df["salary"].fillna(overall_mean_salary)

print("\nAfter handling missing salary:")
print(df)

# --------------------------------------------------
# STEP 3: Remove duplicate records
# --------------------------------------------------
df = df.drop_duplicates()

print("\nAfter removing duplicates:")
print(df)

# --------------------------------------------------
# STEP 4: Filtering (business rules)
# --------------------------------------------------
filtered_df = df[df["salary"] >= 50000]
filtered_df = filtered_df[filtered_df["department"].isin(["IT", "Finance"])]

print("\nAfter filtering:")
print(filtered_df)

# --------------------------------------------------
# STEP 5: GroupBy & Aggregations
# --------------------------------------------------
dept_summary = (
    filtered_df
    .groupby("department")
    .agg(
        avg_salary=("salary", "mean"),
        employee_count=("emp_id", "count")
    )
    .reset_index()
)

print("\nDepartment-wise summary:")
print(dept_summary)

# --------------------------------------------------
# STEP 6: Save outputs
# --------------------------------------------------
filtered_df.to_csv("output/final_clean_data.csv", index=False)
dept_summary.to_csv("output/department_summary.csv", index=False)

print("\nPipeline completed successfully")




