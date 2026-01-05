import pandas as pd


def clean_employee_data(df):
    """
    Clean employee data and generate summary.
    """

    # -----------------------------
    # 1. Fix data types
    # -----------------------------
    df["emp_id"] = pd.to_numeric(df["emp_id"], errors="coerce")
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")

    # -----------------------------
    # 2. Handle missing values
    # -----------------------------
    # Drop rows where emp_id or department is missing
    df = df.dropna(subset=["emp_id", "department"])

    # Fill missing names
    df["name"] = df["name"].fillna("Unknown")

    # Fill missing salary using department average
    df["salary"] = df.groupby("department")["salary"].transform(
        lambda x: x.fillna(x.mean())
    )

    # If salary is still missing, fill with overall average
    overall_avg_salary = df["salary"].mean()
    df["salary"] = df["salary"].fillna(overall_avg_salary)

    # -----------------------------
    # 3. Remove duplicate employees
    # -----------------------------
    df = df.drop_duplicates(subset=["emp_id"])

    # -----------------------------
    # 4. Apply business filters
    # -----------------------------
    df = df[df["salary"] >= 50000]
    df = df[df["department"].isin(["IT", "Finance", "HR", "Sales", "Marketing"])]

    # -----------------------------
    # 5. Create department summary
    # -----------------------------
    dept_summary = (
        df.groupby("department")
        .agg(
            avg_salary=("salary", "mean"),
            employee_count=("emp_id", "count")
        )
        .reset_index()
    )

    return df, dept_summary


# ===============================
# MAIN PROGRAM
# ===============================
if __name__ == "__main__":
    # Read CSV
    employees_df = pd.read_csv("data/employees.csv")
    print("Original rows:", len(employees_df))

    # Clean data
    clean_df, summary_df = clean_employee_data(employees_df)

    # Save outputs
    clean_df.to_csv("output/final_clean_data.csv", index=False)
    summary_df.to_csv("output/department_summary.csv", index=False)

    print("\n✅ Pipeline completed successfully")
    print("Rows after cleaning:", len(clean_df))
    print("\nSample cleaned data:")
    print(clean_df.head())






