import pandas as pd

def clean_employee_data(
    df: pd.DataFrame,
    min_salary: float = 50000,
    valid_departments: list = ["IT", "Finance"]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and filters employee data.

    Steps:
    1. Handle missing values (salary, name, department, emp_id).
    2. Enforce data types.
    3. Remove duplicates.
    4. Apply business rules (salary threshold + department filter).
    5. Generate department summary.

    Returns:
        filtered_df (pd.DataFrame): Cleaned and filtered employee data.
        dept_summary (pd.DataFrame): Aggregated department-level summary.
    """

    # --- Step 1: Handle missing values ---
    # Fill salary by department mean, then overall mean
    df["salary"] = df.groupby("department")["salary"].transform(
        lambda x: x.fillna(x.mean())
    )
    overall_mean_salary = df["salary"].mean()
    df["salary"] = df["salary"].fillna(overall_mean_salary)

    # Drop rows missing critical fields
    df = df.dropna(subset=["emp_id", "department"])
    df["name"] = df["name"].fillna("Unknown")

    # --- Step 2: Enforce data types ---
    df["emp_id"] = pd.to_numeric(df["emp_id"], errors="coerce").astype("Int64")
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")

    # --- Step 3: Remove duplicates ---
    df = df.drop_duplicates(subset=["emp_id"], keep="first")

    # --- Step 4: Apply business rules ---
    filtered_df = df[(df["salary"] >= min_salary) & (df["department"].isin(valid_departments))]

    # --- Step 5: GroupBy & Aggregations ---
    dept_summary = (
        filtered_df.groupby("department")
        .agg(
            avg_salary=("salary", "mean"),
            min_salary=("salary", "min"),
            max_salary=("salary", "max"),
            employee_count=("emp_id", "count")
        )
        .reset_index()
    )

    return filtered_df, dept_summary


# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------
if __name__ == "__main__":
    # Step A: Read employee data
    df = pd.read_csv("data/employees.csv")
    print("Original rows:", len(df))

    # Step B: Run cleaning pipeline
    cleaned_data, summary = clean_employee_data(df)

    # Step C: Save outputs
    cleaned_data.to_csv("output/final_clean_data.csv", index=False)
    summary.to_csv("output/department_summary.csv", index=False)

    # Step D: Console dashboard
    print("\n✅ Pipeline completed successfully")
    print("Rows after cleaning:", len(cleaned_data))
    print("\nDepartment Summary:")
    print("\nSample of Cleaned Data:")
    print(cleaned_data.head())




