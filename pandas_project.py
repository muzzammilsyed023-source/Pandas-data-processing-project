import pandas as pd

# --------------------------------------------------
# STEP 1: Read employee data from CSV
# --------------------------------------------------
df = pd.read_csv("data/employees.csv")

print("Original Data:")
print(df)

# --------------------------------------------------
# STEP 2: Handle missing salary values
# Logic:
# 1. Fill using department-wise average
# 2. If still NaN, fill using overall average
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
# STEP 4: FILTERING (BUSINESS RULES)
# --------------------------------------------------
# Rule 1: Keep only employees with salary >= 50000
filtered_df = df[df["salary"] >= 50000]

# Rule 2: Keep only IT and Finance departments
filtered_df = filtered_df[filtered_df["department"].isin(["IT", "Finance"])]

print("\nAfter applying filters (salary >= 50000 and IT/Finance only):")
print(filtered_df)

# --------------------------------------------------
# STEP 5: Sorting data
# --------------------------------------------------
filtered_df = filtered_df.sort_values(by="salary", ascending=False)

# --------------------------------------------------
# STEP 6: Create derived column
# --------------------------------------------------
filtered_df["salary_category"] = filtered_df["salary"].apply(
    lambda x: "High" if x >= 60000 else "Medium"
)

print("\nFinal transformed data:")
print(filtered_df)

# --------------------------------------------------
# STEP 7: Save cleaned & filtered data
# --------------------------------------------------
filtered_df.to_csv("output/final_clean_data.csv", index=False)

print("\nPipeline completed successfully")



