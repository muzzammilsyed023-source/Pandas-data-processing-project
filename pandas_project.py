import pandas as pd
import requests
employees_df = pd.read_csv("data/employees.csv")
print(employees_df.head())
print(employees_df.info())
employees_df["salary"] = employees_df["salary"].fillna(
    employees_df["salary"].mean()
)
employees_df = employees_df.drop_duplicates()
api_url = "https://dummyjson.com/users"
response = requests.get(api_url)
api_data = response.json()
users = api_data["users"]
api_df = pd.DataFrame(users)
api_df = api_df[["id", "age", "address"]]
api_df["city"] = api_df["address"].apply(lambda x: x["city"])
api_df = api_df.drop(columns=["address"])
final_df = employees_df.merge(
    api_df,
    left_on="emp_id",
    right_on="id",
    how="left"
)
final_df = final_df.drop(columns=["id"])
final_df = final_df.sort_values(by="salary", ascending=False)

final_df["salary_category"] = final_df["salary"].apply(
    lambda x: "High" if x >= 60000 else "Medium"
)
final_df.to_csv("output/final_clean_data.csv", index=False)
print("Pipeline completed successfully")
