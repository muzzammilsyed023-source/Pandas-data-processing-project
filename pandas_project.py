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

# Fill missing salary using department average
df["salary"] = df.groupby("department")["salary"].transform(
    lambda x: x.fillna(x.mean())
)

# Fill remaining NaN (edge case: department has all NaN)
overall_mean_salary = df["salary"].mean()
df["salary"] = df["salary"].fillna(overall_mean_salary)

print("\nAfter handling missing salary:")
print(df)

# --------------------------------------------------
# STEP 3: Remove duplicate records
# --------------------------------------------------
df = df.drop_duplicates()

print("\nAfter removing duplicate rows:")
print(df)

# --------------------------------------------------
# STEP 4: Sort employees by salary (descending)
# --------------------------------------------------
df = df.sort_values(by="salary", ascending=False)

# --------------------------------------------------
# STEP 5: Create derived column (business logic)
# --------------------------------------------------
df["salary_category"] = df["salary"].apply(
    lambda x: "High" if x >= 60000 else "Medium"
)

print("\nFinal transformed data:")
print(df)

# --------------------------------------------------
# STEP 6: Save cleaned data to output folder
# --------------------------------------------------
df.to_csv("output/final_clean_data.csv", index=False)

print("\nPipeline completed successfully")


