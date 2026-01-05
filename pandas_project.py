import pandas as pd
from typing import Tuple, List


def clean_employee_data(
    df: pd.DataFrame,
    min_salary: float = 50000,
    valid_departments: List[str] | None = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and filters employee data.

    Steps:
    1. Enforce data types.
    2. Handle missing values.
    3. Remove duplicates.
    4. Apply business rules.
    5. Generate department-level summary.

    Returns:
        filtered_df: Cleaned employee-level data
        dept_summary: Aggregated department summary
    """

    if valid_departments is None:
        valid_departments = ["IT", "Finance", "HR", "Sales", "Marketing"]

    # -----------------------------
    # Step 1: Enforce data types
    # -----------------------------
    df["emp_id"] = pd.to_numeric(df["emp_id"], errors="coerce").astype("Int64")
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")

    # -----------------------------
    # Step 2: Handle missing values
    # -----------------------------
    df = df.dropna(subset=["emp_id", "department"])
    df["name"] = df["name"].fillna("Unknown")

    # Fill salary using department mean
    df["salary"] = df.groupby("department")["salary"].transform(
        lambda x: x.fillna(x.mean())
    )

    # Fallback to overall mean salary
    overall_mean_salary = df["salary"].mean()
    df["salary"] = df["salary"].fillna(overall_mean_salary)

    # -----------------------------
    # Step 3: Remove duplicates
    # -----------------------------
    df = df.drop_duplicates(subset=["emp_id"], keep="first")

    # -----------------------------
    # Step 4: Apply business rules
    # -----------------------------
    filtered_df = df[
        (df["salary"] >= min_salary)
        & (df["department"].isin(valid_departments))
    ]

    # -----------------------------
    # Step 5: GroupBy & Aggregation
    # -----------------------------
    dept_summary = (
        filtered_df.groupby("department")
        .agg(
            avg_salary=("salary", "mean"),
            min_salary=("salary", "min"),
            max_salary=("salary", "max"),
            employee_count=("emp_id", "count"),
        )
        .reset_index()
    )

    return filtered_df, dept_summary


# ==================================================
# MAIN EXECUTION
# ==================================================
if __name__ == "__main__":
    # Read input data
    df = pd.read_csv("data/employees.csv")
    print("Original rows:", len(df))

    # Run pipeline
    cleaned_data, summary = clean_employee_data(df)

    # Save outputs
    cleaned_data.to_csv("output/final_clean_data.csv", index=False)
    summary.to_csv("output/department_summary.csv", index=False)

    # Console summary
    print("\n✅ Pipeline completed successfully")
    print("Rows after cleaning:", len(cleaned_data))
    print("\nSample cleaned data:")
    print(cleaned_data.head())





