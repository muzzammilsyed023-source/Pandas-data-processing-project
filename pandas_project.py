import pandas as pd


def clean_employee_data(df):
    """
    Cleans employee data and returns:
    1) Cleaned employee-level data
    2) Department-level summary
    """
    df = df.copy()

    # Convert required columns to numeric
    df.loc[:, "emp_id"] = pd.to_numeric(df["emp_id"], errors="coerce")
    df.loc[:, "salary"] = pd.to_numeric(df["salary"], errors="coerce")

    # Drop rows missing critical fields
    df = df.dropna(subset=["emp_id", "department"]).copy()

    # Fill missing names
    df.loc[:, "name"] = df["name"].fillna("Unknown")

    # Fill missing salary using department average
    df.loc[:, "salary"] = (
        df.groupby("department")["salary"]
        .transform(lambda x: x.fillna(x.mean()))
    )

    # Fallback to overall average salary
    overall_avg_salary = df["salary"].mean()
    df.loc[:, "salary"] = df["salary"].fillna(overall_avg_salary)

    # Remove duplicate employees
    df = df.drop_duplicates(subset=["emp_id"]).copy()

    # Apply business rules
    df = df.loc[df["salary"] >= 50000]
    df = df.loc[df["department"].isin(
        ["IT", "Finance", "HR", "Sales", "Marketing"]
    )]

    # Sort output by employee id (ascending)
    df = df.sort_values(by="emp_id", ascending=True).reset_index(drop=True)

    # Create department-level summary
    dept_summary = (
        df.groupby("department", as_index=False)
        .agg(
            avg_salary=("salary", "mean"),
            employee_count=("emp_id", "count")
        )
    )

    return df, dept_summary


if __name__ == "__main__":
    try:
        # Read input data
        employees_df = pd.read_csv("data/employees.csv")

        # Run cleaning pipeline
        clean_df, summary_df = clean_employee_data(employees_df)

        # Check memory usage of cleaned data
        memory_mb = clean_df.memory_usage(deep=True).sum() / (1024 ** 2)
        print(f"Cleaned DataFrame memory usage: {memory_mb:.2f} MB")

        # Save outputs
        clean_df.to_csv("output/final_clean_data.csv", index=False)
        summary_df.to_csv("output/department_summary.csv", index=False)

        print("✅ Pipeline completed successfully")

    except FileNotFoundError:
        print("❌ Error: employees.csv file not found in the data/ folder")

    except pd.errors.EmptyDataError:
        print("❌ Error: employees.csv is empty")

    except Exception as e:
        print(f"❌ Unexpected error occurred: {e}")










