import boto3
import pandas as pd
import matplotlib.pyplot as plt
import io
import os
from datetime import datetime

# --- Config ---
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "cur_db")
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", "s3://my-athena-results-bucket-project-demo/")
S3_BUCKET = os.environ.get("REPORT_BUCKET", "my-athena-results-bucket-project-demo")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

athena = boto3.client("athena")
s3 = boto3.client("s3")
sns = boto3.client("sns")


def run_athena_query(query, database=ATHENA_DATABASE):
    """Run an Athena query and return results as Pandas DataFrame."""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    execution_id = response["QueryExecutionId"]

    # Wait for query to finish
    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

    if state != "SUCCEEDED":
        raise Exception(f"Athena query failed: {state}")

    # Download results
    results = athena.get_query_results(QueryExecutionId=execution_id)
    cols = [c["VarCharValue"] for c in results["ResultSet"]["Rows"][0]["Data"]]
    data = []
    for row in results["ResultSet"]["Rows"][1:]:
        data.append([d.get("VarCharValue", None) for d in row["Data"]])

    return pd.DataFrame(data, columns=cols)


def save_chart(df, chart_type, filename, x_col, y_col):
    """Save a chart to S3."""
    plt.figure(figsize=(8, 5))
    if chart_type == "bar":
        df.plot(x=x_col, y=y_col, kind="bar", legend=False)
    elif chart_type == "line":
        df.plot(x=x_col, y=y_col, kind="line", marker="o", legend=False)

    plt.title(filename.replace("_", " ").title())
    plt.xlabel(x_col)
    plt.ylabel(y_col)

    # Save to buffer
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)

    # Upload to S3
    s3.put_object(Bucket=S3_BUCKET, Key=f"reports/{filename}.png", Body=buf, ContentType="image/png")

    return f"https://{S3_BUCKET}.s3.amazonaws.com/reports/{filename}.png"


def lambda_handler(event, context):
    # 1. Run queries
    queries = {
        "cost_by_service": "SELECT line_item_product_code, SUM(line_item_unblended_cost) as cost FROM cur_cur_daily_parquet GROUP BY line_item_product_code ORDER BY cost DESC LIMIT 10;",
        "cost_trend": "SELECT bill_billing_period_start_date, SUM(line_item_unblended_cost) as cost FROM cur_cur_daily_parquet GROUP BY bill_billing_period_start_date ORDER BY bill_billing_period_start_date;",
        "performance_vs_cost": "SELECT line_item_usage_amount as usage, line_item_unblended_cost as cost FROM cur_cur_daily_parquet LIMIT 50;",
    }

    chart_links = {}
    summary_text = []

    for name, query in queries.items():
        df = run_athena_query(query)

        if df.empty:
            summary_text.append(f"{name}: No data available.")
            continue

        # Convert numeric cols
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except:
                pass

        # Save chart
        if name == "cost_by_service":
            link = save_chart(df, "bar", name, "line_item_product_code", "cost")
            summary_text.append(f"Top services by cost:\n{df.to_string(index=False)}")
        elif name == "cost_trend":
            link = save_chart(df, "line", name, "bill_billing_period_start_date", "cost")
            summary_text.append(f"Cost trend over time:\n{df.to_string(index=False)}")
        elif name == "performance_vs_cost":
            link = save_chart(df, "scatter", name, "usage", "cost")
            summary_text.append(f"Performance vs Cost sample:\n{df.to_string(index=False)}")
        else:
            link = None

        if link:
            chart_links[name] = link

    # 2. Prepare SNS message
    today = datetime.utcnow().strftime("%Y-%m-%d")
    message = f"AWS Daily Cost Report ({today})\n\n"
    message += "\n\n".join(summary_text)
    message += "\n\nCharts:\n"
    for k, v in chart_links.items():
        message += f"{k}: {v}\n"

    # 3. Send SNS notification
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"AWS Daily Cost Report - {today}",
        Message=message,
    )

    return {"status": "done", "charts": chart_links}
