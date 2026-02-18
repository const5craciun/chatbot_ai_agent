import re
import pandas as pd
from smolagents import tool
from sqlalchemy import text
from datetime import datetime
import matplotlib.pyplot as plt
from scripts.utils.db import engine 
from scripts.utils.tool_logger import log_tool_usage

tool_usage_log = []

# ---------- Simple aggregate SQL query toool ---------
@tool
def aggregate_metric_simple_where(
    metric: str,
    agg: str,
    where_clause: str = ""
) -> str:
    """
    PURPOSE:
        Use this tool to compute a single aggregated numeric value
        from the table `marketing_data`.
    
    WHEN TO USE:
        - The user asks for a total, sum, average, minimum, maximum, or count.
        - The question refers to a specific filter such as year, quarter_number,
          month_number,month_name, product, country, media_category, campaign_name, etc.
        - The question requires exactly ONE numeric result.
        - The user does NOT request ranking, grouping, or comparisons across categories.
    
    DO NOT USE:
        - For grouped results (e.g., "by media_category", "by campaign_name").
        - For top-N or ranking queries (e.g., "Top 5 campaigns").
        - For time trends (e.g., "revenue over time", "trend by month").
        - For comparisons across multiple groups in a single query.
    
    TABLE:
        marketing_data
    
    DATAFRAME SCHEMA:
        year (BIGINT)
        quarter (TEXT)                  # Example: "2020 Q3"
        month (TEXT)                    # Example: "2020M08"
        week (BIGINT)
        date (TIMESTAMP)
        country (TEXT)
        media_category (TEXT)
        media_name (TEXT)
        communication (TEXT)
        campaign_category (TEXT)
        product (TEXT)
        campaign_name (TEXT)
        revenue (DOUBLE PRECISION)
        cost (DOUBLE PRECISION)
        profit (DOUBLE PRECISION)
        roi (DOUBLE PRECISION)
        margin (DOUBLE PRECISION)
        quarter_number (BIGINT)         # 1–4
        month_number (BIGINT)           # 1–12
        month_name (TEXT)               # Example: "August"
    
    NUMERIC COLUMNS (valid for `metric`):
        revenue, cost, profit, roi, margin
    
    ALLOWED AGGREGATIONS:
        sum, avg, min, max, count
    
    FILTERING RULE:
        Provide filtering logic in `where_clause` WITHOUT the word WHERE.
    
        Examples:
            "year = 2023"
            "year = 2023 AND quarter_number = 2"
            "year = 2022 AND product = 'Product 1'"
            "media_category = 'online'"
            "month_name = 'August' AND country = 'DK'"
    
    IMPORTANT RULES:
        - Column names must be lowercase.
        - String values must be wrapped in single quotes.
        - Do NOT include GROUP BY.
        - Q2 corresponds to quarter_number = 2.
        - This tool always returns exactly ONE scalar numeric value.
    
    Args:
        metric (str):
            Name of numeric column to aggregate.
            Must be one of:
                revenue, cost, profit, roi, margin.
    
        agg (str):
            Aggregation function to apply.
            Must be one of:
                sum, avg, min, max, count.
    
        where_clause (str, optional):
            SQL filtering condition WITHOUT the word WHERE.
            Example:
                "year = 2023"
                "year = 2023 AND quarter_number = 2"
                "product = 'Product 1'"
    
    Returns:
        A single numeric value as a string.
        If no matching rows exist, returns "No results found."
    """

    allowed_metrics = {"revenue", "cost", "profit", "roi", "margin"}
    allowed_aggs = {"sum", "avg", "min", "max", "count"}

    if metric not in allowed_metrics:
        return f"Invalid metric. Allowed: {allowed_metrics}"

    if agg not in allowed_aggs:
        return f"Invalid aggregation. Allowed: {allowed_aggs}"

    where_sql = f"WHERE {where_clause}" if where_clause else ""

    query = f"""
        SELECT {agg}({metric})
        FROM marketing_data
        {where_sql}
    """
    
    # Log tool usage for later checks
    log_tool_usage(
        tool_name="aggregate_metric_simple_where",
        metadata={
            "metric": metric,
            "agg": agg,
            "where_clause": where_clause
        }
    )


    with engine.connect() as con:
        result = con.execute(text(query)).fetchone()

    if result is None:
        return "No results."

    return str(result[0])


# ----------- Aggregate with grouping and order tool ---------------
    
@tool
def aggregate_with_grouping(
    metric: str,
    agg: str,
    group_by: str = "",
    where_clause: str = "",
    order_desc: bool = True,
    limit: int = 0
) -> str:

    """
    PURPOSE:
        Use this tool to execute aggregated SQL queries on the table `marketing_data`,
        with optional filtering, grouping, ordering, and limiting of results.
    
    WHEN TO USE:
        - The user asks for total, sum, average, minimum, maximum, or count.
        - The question includes filters such as year, quarter_number, month_number,
          product, country, media_category, campaign_name, etc.
        - The user requests ranked or grouped results (e.g., "Top 5 campaigns by revenue").
        - The user asks for comparisons across categories
          (e.g., "highest profit by media_category in Q2 2023").
        - The question requires aggregation but NOT time-series plotting.
    
    DO NOT USE:
        - For time-based trends (e.g., "revenue over time", "trend by month").
        - For visualizations (use plotting tool instead).
    
    TABLE:
        marketing_data
    
    DATAFRAME SCHEMA:
        year (BIGINT)
        quarter (TEXT)                  # Example: "2020 Q3"
        month (TEXT)                    # Example: "2020M08"
        week (BIGINT)
        date (TIMESTAMP)
        country (TEXT)
        media_category (TEXT)
        media_name (TEXT)
        communication (TEXT)
        campaign_category (TEXT)
        product (TEXT)
        campaign_name (TEXT)
        revenue (DOUBLE PRECISION)
        cost (DOUBLE PRECISION)
        profit (DOUBLE PRECISION)
        roi (DOUBLE PRECISION)
        margin (DOUBLE PRECISION)
        quarter_number (BIGINT)         # 1–4
        month_number (BIGINT)           # 1–12
        month_name (TEXT)               # Example: "August"
    
    NUMERIC COLUMNS (valid for `metric`):
        revenue, cost, profit, roi, margin
    
    ALLOWED AGGREGATIONS:
        sum, avg, min, max, count
    
    FILTERING RULE:
        The `where_clause` must contain SQL conditions WITHOUT the word WHERE.
    
        Examples:
            "year = 2023"
            "year = 2023 AND quarter_number = 2"
            "media_category = 'online'"
            "year = 2022 AND product = 'Product 1'"
            "month_number = 8 AND country = 'DK'"
    
    GROUPING:
        If `group_by` is provided:
            - A GROUP BY clause is applied.
            - Results are ordered by the aggregated value.
            - Optional LIMIT can restrict the number of returned rows.
            - Suitable for "Top N" or category comparison queries.
    
    ORDERING:
        - If grouped, results are ordered by the aggregated value.
        - `order_desc=True` sorts descending (default).
        - `order_desc=False` sorts ascending.
    
    LIMIT:
        - If `limit > 0`, applies a LIMIT clause.
        - If 0, returns all matching rows.
    
    Args:
        metric (str):
            Name of numeric column to aggregate.
            Must be one of:
                revenue, cost, profit, roi, margin.
    
        agg (str):
            Aggregation function to apply.
            Must be one of:
                sum, avg, min, max, count.
    
        group_by (str, optional):
            Column to group results by.
            Examples:
                "campaign_name"
                "media_category"
                "product"
                "country"
    
        where_clause (str, optional):
            SQL filtering condition WITHOUT the word WHERE.
    
        order_desc (bool, optional):
            If True, sort results descending by aggregated value.
            If False, sort ascending.
            Default: True.
    
        limit (int, optional):
            Maximum number of rows to return.
            Default: 0 (no limit).
    
    Returns:
        - If a single scalar value → returns the numeric result as a string.
        - If grouped → returns rows formatted as:
            (<group_value>, <aggregated_value>)
        - If no matching data → returns "No results found."
    """

    allowed_metrics = {"revenue", "cost", "profit", "roi", "margin"}
    allowed_aggs = {"sum", "avg", "min", "max", "count"}

    if metric not in allowed_metrics:
        return f"Invalid metric. Allowed: {allowed_metrics}"

    if agg not in allowed_aggs:
        return f"Invalid aggregation. Allowed: {allowed_aggs}"

    where_sql = f"WHERE {where_clause}" if where_clause else ""
    group_sql = f"GROUP BY {group_by}" if group_by else ""

    order_sql = ""
    if group_by:
        direction = "DESC" if order_desc else "ASC"
        order_sql = f"ORDER BY {agg}({metric}) {direction}"

    limit_sql = f"LIMIT {limit}" if limit > 0 else ""

    query = f"""
        SELECT
            {group_by + ',' if group_by else ''} 
            {agg}({metric}) AS value
        FROM marketing_data
        {where_sql}
        {group_sql}
        {order_sql}
        {limit_sql}
    """
    log_tool_usage(
        tool_name="aggregate_with_grouping",
        metadata={
            "metric": metric,
            "agg": agg,
            "where_clause": where_clause
        }
    )

    with engine.connect() as con:
        result = con.execute(text(query))
        rows = result.fetchall()

    if not rows:
        return "No results found."

    if len(rows) == 1 and len(rows[0]) == 1:
        return str(rows[0][0])

    return "\n".join(str(row) for row in rows)


# ------------- Tool that plots a trend ----------------
    
@tool
def plot_trend(
    metrics: str,
    time_dimension: str,
    where_clause: str = ""
) -> str:
    """
    PURPOSE:
        Use this tool to generate a time-based trend plot by aggregating numeric metrics
        from the table `marketing_data` over a specified time dimension.
    
    WHEN TO USE:
        - The question involves time-based aggregation (e.g., "by month", "by quarter", "by year").
        - The user asks for a trend (e.g., "trend", "over time", "evolution").
        - The user requests visualization (e.g., "plot", "chart", "graph", "show trend").
        - The user wants revenue, cost, profit, ROI, or margin development across time.
    
    DO NOT USE:
        - For single numeric answers (use aggregation tool instead).
        - For grouped category comparisons without a time dimension.
        - For ranking or top-N queries without time context.
        - For static comparisons between categories in one period.
    
    TABLE:
        marketing_data
    
    DATAFRAME SCHEMA:
        year (BIGINT)
        quarter (TEXT)                  # Example: "2020 Q3"
        month (TEXT)                    # Example: "2020M08"
        week (BIGINT)
        date (TIMESTAMP)
        country (TEXT)
        media_category (TEXT)
        media_name (TEXT)
        communication (TEXT)
        campaign_category (TEXT)
        product (TEXT)
        campaign_name (TEXT)
        revenue (DOUBLE PRECISION)
        cost (DOUBLE PRECISION)
        profit (DOUBLE PRECISION)
        roi (DOUBLE PRECISION)
        margin (DOUBLE PRECISION)
        quarter_number (BIGINT)         # 1–4
        month_number (BIGINT)           # 1–12
        month_name (TEXT)               # Example: "August"
    
    ALLOWED METRICS:
        revenue, cost, profit, roi, margin
    
    ALLOWED TIME DIMENSIONS (for grouping):
        month_name
        quarter_number
        year
        date
    
    RECOMMENDED TIME USAGE:
        - Use month_name for monthly trends within a year.
        - Use quarter_number for quarterly trends.
        - Use year for yearly trends.
        - Use date for daily-level trends.
    
    BEHAVIOR:
        - Plot chronologically. Example: January, February, March. 
        - Aggregates selected metrics using SUM().
        - Groups by the provided time_dimension.
        - Orders results chronologically (ascending).
        - Generates a line plot for each metric.
        - Returns confirmation including the saved file path.
        - Returns "No data found." if no matching rows exist.
    
    FILTERING RULE:
        `where_clause` must contain SQL conditions WITHOUT the word WHERE.
    
        Examples:
            "year = 2023"
            "year = 2023 AND product = 'Product 1'"
            "country = 'DK' AND quarter_number = 2"
            "year = 2022 AND media_category = 'online'"
    
    Args:
        metrics (str):
            Comma-separated numeric columns to aggregate and plot.
            Examples:
                "revenue"
                "revenue,cost"
                "profit,roi"
    
        time_dimension (str):
            Time column used for grouping.
            Must be one of:
                month_name, quarter_number, year, date
    
        where_clause (str, optional):
            SQL filtering condition WITHOUT the word WHERE.
    
    Returns:
        - Returns plot and saves it at specified location.
        - Returns "No data found." if no matching rows exist.
    """


    allowed_metrics = {"revenue", "cost", "profit", "roi"}
    allowed_time_dims = {"month_name","quarter_number", "year"}

    metric_list = [m.strip() for m in metrics.split(",")]

    for m in metric_list:
        if m not in allowed_metrics:
            return f"Invalid metric: {m}"

    if time_dimension not in allowed_time_dims:
        return f"Invalid time dimension: {time_dimension}"

    where_sql = f"WHERE {where_clause}" if where_clause else ""

    agg_sql = ", ".join([f"SUM({m}) AS {m}" for m in metric_list])

    if time_dimension == "month_name":
        select_dim = "month_name, month_number"
        group_sql = "GROUP BY month_name, month_number"
        order_sql = "ORDER BY month_number"
    elif time_dimension == "quarter_number":
        select_dim = "quarter_number"
        group_sql = "GROUP BY quarter_number"
        order_sql = "ORDER BY quarter_number"
    elif time_dimension == "year":
        select_dim = "year"
        group_sql = "GROUP BY year"
        order_sql = "ORDER BY year"
    elif time_dimension == "date":
        select_dim = "date"
        group_sql = "GROUP BY date"
        order_sql = "ORDER BY date"
    else:
        return "Invalid time dimension."


    query = f"""
        SELECT {time_dimension}, {agg_sql}
        FROM marketing_data
        {where_sql}
        {group_sql}
        {order_sql}
    """

    df = pd.read_sql(query, engine)
    
    safe_where = re.sub(r"[^a-zA-Z0-9_]", "_", where_clause)
    safe_metrics = "_".join(metric_list)
    
    filename = f"{safe_metrics}_{time_dimension}_{safe_where}.png"
    csv_path = f"scripts/plots_output/{safe_metrics}_{time_dimension}_{safe_where}.csv"
    
    if df.empty:
        return "No data found."
    else:
        df.to_csv(csv_path, index=False)

    for m in metric_list:
        plt.plot(df[time_dimension], df[m], label=m)

    plt.legend()
    plt.xlabel(time_dimension)
    plt.title(f"Plot of {safe_metrics} over a {time_dimension} for {where_clause}")
    plt.xticks(rotation=45)
    plt.savefig(f"scripts/plots_output/{filename}")
    plt.tight_layout()
    plt.show()
    plt.close()  

    return "Plot generated successfully."


# ---------- Scatter plot tool -------


@tool
def plot_scatter_relationship(
    x_metric: str,
    y_metric: str,
    category_dimension: str = "",
    year: int = None
) -> str:
    """
    PURPOSE:
        Generate a scatter plot to analyze the relationship between two numeric
        marketing metrics using the preloaded pandas DataFrame `marketing_df`.
    
    WHEN TO USE:
        - The user wants to understand the relationship between two KPIs.
        - The user asks about correlation (e.g., "Is cost related to revenue?").
        - The user wants to compare performance efficiency (e.g., ROI vs margin).
        - The user wants to visually compare campaign or media performance
          across two numeric variables.
        - The user mentions "scatter plot", "relationship", "correlation",
          "compare X vs Y", or similar phrasing.
    
    DO NOT USE:
        - For time-based trends (use a line/trend tool instead).
        - For ranking or top-N comparisons.
        - For single aggregated numeric answers.
        - For categorical bar comparisons without numeric relationships.
    
    DATA SOURCE:
        Uses the preloaded pandas DataFrame: `marketing_df`
        (No SQL queries are generated.)
    
    DATAFRAME SCHEMA:
        year (int)
        quarter (str)
        month (str)
        week (int)
        date (datetime)
        country (str)
        media_category (str)
        media_name (str)
        communication (str)
        campaign_category (str)
        product (str)
        campaign_name (str)
        revenue (float)
        cost (float)
        profit (float)
        roi (float)
        margin (float)
        quarter_number (int)
        month_number (int)
        month_name (str)
    
    ALLOWED METRICS:
        revenue, cost, profit, roi, margin
    
    ALLOWED CATEGORY DIMENSIONS:
        campaign_name
        campaign_category
        media_category
        product
        country
    
    BEHAVIOR:
        - Filters data optionally by year.
        - Plots x_metric on the x-axis.
        - Plots y_metric on the y-axis.
        - If category_dimension is provided, different categories are
          plotted with separate markers and a legend.
        - Returns "No data found." if the filtered dataset is empty.
        - Displays the scatter plot.
    
    Args:
        x_metric (str):
            Numeric column to plot on x-axis.
    
        y_metric (str):
            Numeric column to plot on y-axis.
    
        category_dimension (str, optional):
            Categorical column used to segment points visually.
    
        year (int, optional):
            Filters dataset to a specific year.
    
    RETURNS:
        Confirmation string after generating the scatter plot,
        or "No data found." if the filtered dataset is empty.
    """

    allowed_metrics = {"revenue", "cost", "profit", "roi", "margin"}
    allowed_categories = {
        "campaign_name",
        "campaign_category",
        "media_category",
        "product",
        "country"
    }

    if x_metric not in allowed_metrics:
        return f"Invalid x_metric: {x_metric}"

    if y_metric not in allowed_metrics:
        return f"Invalid y_metric: {y_metric}"

    if category_dimension and category_dimension not in allowed_categories:
        return f"Invalid category dimension: {category_dimension}"

    df = marketing_df.copy()

    if year is not None:
        df = df[df["year"] == year]

    if df.empty:
        return "No data found."

    plt.figure()

    if category_dimension:
        for cat in df[category_dimension].dropna().unique():
            subset = df[df[category_dimension] == cat]
            plt.scatter(subset[x_metric], subset[y_metric], label=cat)
        plt.legend()
    else:
        plt.scatter(df[x_metric], df[y_metric])

    plt.xlabel(x_metric)
    plt.ylabel(y_metric)
    plt.title(f"{y_metric} vs {x_metric}")
    plt.tight_layout()
    plt.show()
    plt.close()

    return "Scatter plot generated successfully."

# ------ 

@tool
def aggregate_metric_structured(
    metric: str,
    agg: str,
    filters: dict | None = None
) -> str:
    """
    PURPOSE:
        Compute a single aggregated numeric value from the table `marketing_data`
        using structured dictionary-based filters (NO raw SQL allowed).

    WHEN TO USE:
        - The user asks for a total, sum, average, minimum, maximum, or count.
        - The question requires exactly ONE numeric result.
        - The query refers to specific filters (year, product, country, campaign, etc.).
        - The user does NOT request grouping, ranking, comparisons, or trends.

    DO NOT USE:
        - For grouped results (e.g., "by media_category", "by campaign_name").
        - For top-N or ranking queries.
        - For time-series trends (e.g., "revenue over time").
        - For comparisons across multiple categories.
        - When more than one aggregated result is required.

    TABLE:
        marketing_data

    DATAFRAME SCHEMA:
        year (BIGINT)
        quarter (TEXT)
        month (TEXT)
        week (BIGINT)
        date (TIMESTAMP)
        country (TEXT)
        media_category (TEXT)
        media_name (TEXT)
        communication (TEXT)
        campaign_category (TEXT)
        product (TEXT)
        campaign_name (TEXT)
        revenue (DOUBLE PRECISION)
        cost (DOUBLE PRECISION)
        profit (DOUBLE PRECISION)
        roi (DOUBLE PRECISION)
        margin (DOUBLE PRECISION)
        quarter_number (BIGINT)
        month_number (BIGINT)
        month_name (TEXT)

    VALID METRICS:
        revenue, cost, profit, roi, margin

    VALID AGGREGATIONS:
        sum, avg, min, max, count

    VALID FILTER COLUMNS:
        year
        quarter_number
        month_number
        month_name
        product
        country
        media_category
        campaign_name
        campaign_category

    FILTER FORMAT:
        Filters must be passed as a dictionary with column-value equality pairs.

        Example:
            {"year": 2023}
            {"year": 2023, "media_category": "online"}
            {"month_name": "August", "country": "DK"}

        Rules:
            - Keys must match allowed filter columns exactly.
            - Values must be properly typed:
                * Integers for numeric columns (e.g., year, month_number)
                * Strings for text columns (e.g., product, country)
            - Only equality filtering is supported.
            - No SQL syntax is allowed inside filters.
            - Do NOT pass raw SQL strings.

    BEHAVIOR:
        - Builds a parameterized SQL query safely.
        - Applies equality filters only.
        - Executes query against marketing_data.
        - Returns exactly ONE scalar numeric result as a string.
        - Returns "No results found." if no matching rows exist.

    Args:
        metric (str):
            Name of numeric column to aggregate.

        agg (str):
            Aggregation function to apply.

        filters (dict, optional):
            Dictionary of equality filters.

    RETURNS:
        String representation of a single numeric value.
        Or "No results found." if no data matches.
    """
    
    allowed_metrics = {"revenue", "cost", "profit", "roi", "margin"}
    allowed_aggs = {"sum", "avg", "min", "max", "count"}

    allowed_filter_columns = {
        "year",
        "quarter_number",
        "month_number",
        "month_name",
        "product",
        "country",
        "media_category",
        "campaign_name",
        "campaign_category"
    }

    if metric not in allowed_metrics:
        return f"Invalid metric. Allowed: {allowed_metrics}"

    if agg not in allowed_aggs:
        return f"Invalid aggregation. Allowed: {allowed_aggs}"

    query = f"SELECT {agg}({metric}) FROM marketing_data"
    params = {}

    if filters:
        conditions = []
        for col, value in filters.items():

            if col not in allowed_filter_columns:
                return f"Invalid filter column: {col}"

            param_name = f"param_{col}"
            conditions.append(f"{col} = :{param_name}")
            params[param_name] = value

        where_sql = " WHERE " + " AND ".join(conditions)
        query += where_sql

    log_tool_usage(
        tool_name="aggregate_metric_structured",
        metadata={
            "metric": metric,
            "agg": agg,
            "filters": filters
        }
    )
    print(query, params)
    with engine.connect() as con:
        result = con.execute(text(query), params).fetchone()

    if result is None or result[0] is None:
        return "No results found."

    return str(result[0])