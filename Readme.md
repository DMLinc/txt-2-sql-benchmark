## Questions

- Existing Benchmark Questions - What are they
- Our own Categories of questions (see below)
    - Which Hypothesis are we testing under each?

## Dataset Generation

- How do we generate the datasets?
    - Dynamically?
    - OR based on our pre-defined questions
- Is the approach Robust? Are the JOINs going to work? Do the values make sense (math-wise)

## Sample Question-Answer Pair Generation

- Generate Answers with our Questions from 3 Azure AI models (3.5, 4, …)

## Live Execution & Testing

- How to test this on the fly?
- Deployment to Streamlit

## Categories of questions:

1. Fanout / fact table - questions that ask for a fanout-ed implementation
2. Non-trivial joins (inequality, multiple fields, non-obvious id, multiple similar ids, many-to-many, self-joins)
3. Period-over-period requests
4. Window function
5. JSON and XML Data Handling
6. Spatial and Geographical Data Analysis
7. Date functions: time zone conversions, time based joins, etc.
8. Complex string matching or conversion
9. Activity schema handling
10. Complex aggregates: HAVING, NULL-safety, group by calculated fields
11. CTE usage / recursive queries

### Sample Questions / Answers

# 1. Fanout

### **Incorrectly Handled Fanout (Without Fact Tables)**

**Question: Calculate the total sales revenue by product category?**

Incorrect SQL Answer:

```
- SELECT c.category_name, SUM(o.amount) AS total_sales
- FROM categories c
- JOIN product_categories pc ON c.id = pc.category_id
- JOIN products p ON pc.product_id = p.id
- JOIN orders o ON p.id = o.product_id
- GROUP BY c.category_name;
```

**Explanation: This query might double-count amount values for orders if products belong to multiple categories, inflating total sales figures.**

### **Correctly Handled Fanout (With Fact Tables)**

**Question: Calculate the total sales revenue by product category using a fact table to correctly aggregate sales data without duplication.**

```
SELECT c.category_name, SUM(f.sales_amount) AS total_sales

FROM category_dim c

JOIN sales_fact f ON c.category_id = f.category_id

GROUP BY c.category_name;
```

**Explanation: This query accurately calculates total sales by category, using a sales fact table that avoids duplication by design.**

**Question: Determine the total number of products sold for each product, using a fact table to correctly handle sales data aggregation.**

```
SELECT p.product_name, SUM(f.quantity_sold) AS total_quantity

FROM product_dim p

JOIN sales_fact f ON p.product_id = f.product_id

GROUP BY p.product_name;
```

**Explanation: This query uses a fact table that records the quantity sold for each product, ensuring accurate aggregation without the risk of duplication due to fanout.**

# 2. Non-trivial joins

# 3. Period-over-period requests

### **Calculate Month-over-Month Sales Growth (Postgres)**

Question: How can you calculate the month-over-month sales growth percentage for each product in a Postgres database?

**Version 1:**

```
SELECT

product_id,

EXTRACT(YEAR FROM sale_date) AS sale_year,

EXTRACT(MONTH FROM sale_date) AS sale_month,

SUM(sales_amount) AS total_sales,

(SUM(sales_amount) - LAG(SUM(sales_amount)) OVER (PARTITION BY product_id ORDER BY EXTRACT(YEAR FROM sale_date), EXTRACT(MONTH FROM sale_date))) / LAG(SUM(sales_amount)) OVER (PARTITION BY product_id ORDER BY EXTRACT(YEAR FROM sale_date), EXTRACT(MONTH FROM sale_date)) * 100 AS mom_growth

FROM

sales

GROUP BY

product_id,

sale_year,

sale_month

ORDER BY

product_id,

sale_year,

Sale_month;
```

**Explanation: This query calculates the month-over-month sales growth for each product. It uses the LAG window function to access the previous row's sales amount, allowing for the calculation of the growth percentage.**

**Version 2:**

```
WITH monthly_sales AS (

SELECT

product_id,

DATE_TRUNC('month', sale_date) AS month,

SUM(sales_amount) AS total_sales

FROM sales

GROUP BY product_id, month

),

previous_month_sales AS (

SELECT

a.product_id,

a.month,

a.total_sales,

b.total_sales AS prev_month_sales

FROM monthly_sales a

LEFT JOIN monthly_sales b ON a.product_id = b.product_id

AND a.month = (b.month + INTERVAL '1 month')

)

SELECT

product_id,

month,

total_sales,

prev_month_sales,

CASE

WHEN prev_month_sales IS NULL THEN NULL

ELSE (total_sales - prev_month_sales) / prev_month_sales * 100

END AS mom_growth_percentage

FROM previous_month_sales

ORDER BY product_id, month;
```

**Explanation:**

- **The monthly_sales CTE (Common Table Expression) calculates the total sales for each product per month.**
- **The previous_month_sales CTE then performs a self-join on monthly_sales to match each month's sales with the previous month's sales for the same product. We achieve the month-over-month comparison by using an interval of 1 month in the join condition.**
- **Finally, we select the results, including the calculated Month-over-Month growth percentage. The CASE statement ensures that we handle cases where there is no previous month's sales data (e.g., the first month in the dataset), avoiding division by zero.**

# 4. Window function

### **Analyzing Sessions of Web Visits with Window Functions**

Question: How do you calculate the duration of each session for web visits, assuming a session ends when a user is inactive for more than 30 minutes?

```
WITH ranked_visits AS (
  SELECT
    user_id,
    visit_timestamp,
    LAG(visit_timestamp) OVER (PARTITION BY user_id ORDER BY visit_timestamp) AS previous_visit_timestamp,
    EXTRACT(EPOCH FROM (visit_timestamp - LAG(visit_timestamp) OVER (PARTITION BY user_id ORDER BY visit_timestamp))) / 60 AS minutes_since_last_visit
  FROM web_visits
),
sessions AS (
  SELECT
    user_id,
    visit_timestamp,
    SUM(CASE WHEN minutes_since_last_visit > 30 OR minutes_since_last_visit IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY visit_timestamp) AS session_id
  FROM ranked_visits
)
SELECT
  user_id,
  session_id,
  MIN(visit_timestamp) AS session_start,
  MAX(visit_timestamp) AS session_end,
  EXTRACT(EPOCH FROM (MAX(visit_timestamp) - MIN(visit_timestamp))) / 60 AS session_duration_minutes
FROM sessions
GROUP BY user_id, session_id
ORDER BY user_id, session_start;
```
**Explanation:**
- **This query first calculates the time difference between consecutive visits for each user to identify session boundaries (defined as a gap of more than 30 minutes). It then assigns a session ID to each visit, incrementing the ID whenever a new session starts. Finally, it aggregates these visits by session to calculate the start, end, and duration of each session.**

### **Evaluating Marketing Lead Attribution Funnels with Window Functions**
Question: How do you determine the conversion funnel for leads coming from various marketing channels, tracking their progress from lead creation to becoming a qualified lead, then to an opportunity, and finally to a closed sale?

```
WITH lead_stages AS (
  SELECT
    lead_id,
    channel,
    stage,
    stage_timestamp,
    LEAD(stage_timestamp) OVER (PARTITION BY lead_id ORDER BY stage_timestamp) AS next_stage_timestamp
  FROM marketing_leads
)
SELECT
  channel,
  COUNT(DISTINCT CASE WHEN stage = 'Lead Created' THEN lead_id END) AS leads_created,
  COUNT(DISTINCT CASE WHEN stage = 'Qualified Lead' THEN lead_id END) AS leads_qualified,
  COUNT(DISTINCT CASE WHEN stage = 'Opportunity' THEN lead_id END) AS opportunities_created,
  COUNT(DISTINCT CASE WHEN stage = 'Closed Sale' THEN lead_id END) AS sales_closed
FROM lead_stages
GROUP BY channel
ORDER BY channel;

```
**Explanation:**

- **This query tracks each lead's progress through the marketing funnel stages. By using the LEAD function, it prepares data that shows when a lead moves to the next stage. It then counts distinct lead IDs at each stage, grouped by marketing channel, to evaluate the effectiveness of each channel in moving leads through the funnel towards a sale.**

# 5. JSON and XML Data Handling

Question 1: How do you extract and aggregate information from a JSON document stored in a PostgreSQL table?

```
CREATE TABLE customer_orders (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    order_info JSONB
);

-- Example JSONB column data: { "items": [{"product": "Book", "quantity": 2}, {"product": "Pen", "quantity": 5}] }

-- Query to extract product names and their total quantities ordered across all orders
SELECT
  jsonb_array_elements(order_info->'items')->>'product' AS product_name,
  SUM((jsonb_array_elements(order_info->'items')->>'quantity')::int) AS total_quantity
FROM customer_orders
GROUP BY product_name;
```

**Explanation:**

- **This query uses the jsonb_array_elements function to expand the JSON array in order_info->'items' into a set of JSONB elements, then extracts the product and quantity fields. It casts quantity to an integer and aggregates the total quantity ordered for each product across all orders.**

# 6. Spatial and Geographical Data Analysis (NOT in v0)

# 7. Date functions: time zone conversions, time based joins, etc.

### **Handling Daylight Saving Time (DST) Adjustments**
Question: How do you account for Daylight Saving Time when converting between time zones?
```
-- Example of converting a timestamp from UTC to a time zone with DST (e.g., Eastern Time)
SELECT
    event_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS event_time_et
FROM global_events;
```

**Explanation:**

- **PostgreSQL automatically handles Daylight Saving Time adjustments for time zones like 'America/New_York' when using the AT TIME ZONE conversion. The result will reflect the correct local time, including any DST shifts.**

### **Time-based Joins in PostgreSQL**
Question: How do you perform a join between two tables based on time intervals, such as finding records within the same hour?
```
CREATE TABLE user_logins (
    user_id INT,
    login_time TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE user_actions (
    user_id INT,
    action_time TIMESTAMP WITHOUT TIME ZONE,
    action VARCHAR(255)
);

-- Example query to find actions within an hour of login
SELECT
    l.user_id,
    l.login_time,
    a.action,
    a.action_time
FROM user_logins l
JOIN user_actions a ON l.user_id = a.user_id
    AND a.action_time BETWEEN l.login_time AND l.login_time + INTERVAL '1 hour';

```

**Explanation:**

- **This query joins the user_logins and user_actions tables on user_id, using a condition that matches actions (action_time) occurring within an hour (INTERVAL '1 hour') after the login time (login_time).**

### **Time Zone Conversions in PostgreSQL**
Question: How do you convert stored timestamps from one time zone to another?
```
CREATE TABLE global_events (
    id SERIAL PRIMARY KEY,
    event_name VARCHAR(255),
    event_time TIMESTAMP WITH TIME ZONE
);

-- Example query to convert UTC to EST
SELECT
    id,
    event_name,
    event_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS event_time_est
FROM global_events;
```
**Explanation:**

- **The AT TIME ZONE clause is used to convert event_time from UTC to EST. The first AT TIME ZONE 'UTC' interprets the stored timestamp as UTC (if not already specified as such), and the second AT TIME ZONE 'America/New_York' converts it to Eastern Standard Time.**

# 8. Complex string matching or conversion

### **BigQuery: Regex Operations - Extracting Domain Names from URLs**
```
SELECT
  REGEXP_EXTRACT(url, 'https?://([^/]+)/') AS domain
FROM webpage_visits;

```
```
SELECT *
FROM inventory
WHERE product_id REGEXP_CONTAINS(product_id, '^[A-Za-z0-9]+$');
```

### **Filtering Rows Based on a Pattern with LIKE**
```
SELECT *
FROM products
WHERE name ILIKE '%eco-friendly%'; -- Case-insensitive search for 'eco-friendly' in the product name
```

### **Extracting Substrings Matching a Pattern**
```
SELECT
  SUBSTRING(email FROM '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') AS valid_email
FROM users;
```

# 9. Activity schema handling

### **BigQuery: Activity Schema Implementation**
```
CREATE TABLE user_activities (
    id STRING NOT NULL,
    user_id INT64 NOT NULL,
    activity_type STRING NOT NULL,
    activity_details STRUCT<
        detail1 STRING,
        detail2 INT64,
        ...
    >,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(created_at);

SELECT user_id, activity_type, ARRAY_LENGTH(activity_details.someArray) AS detailCount
FROM user_activities
WHERE activity_type = 'view';
```

**Explanation**

- **Benefits for BigQuery: BigQuery's approach is well-suited for analytics at scale. The use of partitioning (PARTITION BY DATE(created_at)) enhances query performance and cost efficiency for time-based queries. Structured data types (STRUCT, ARRAY) allow for complex data organization within each activity record.**

### **Snowflake: Handling Activity Schema**
```
CREATE TABLE user_activities (
    id AUTOINCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    activity_type STRING NOT NULL,
    activity_details VARIANT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

SELECT activity_type, COUNT(*)
FROM user_activities
GROUP BY activity_type;
```

**Explanation**

- **Benefits for Snowflake: Snowflake's VARIANT type is ideal for activity_details, accommodating various structured and semi-structured formats. Snowflake's architecture also allows handling large volumes of data efficiently.**

# 10. Complex aggregates: HAVING, NULL-safety, group by calculated fields

### **Example 1: Using HAVING for Filtered Aggregates**

Question: Identify products with an average rating above 4.5, where more than 10 ratings have been submitted.

```
SELECT
  product_id,
  AVG(rating) AS avg_rating,
  COUNT(rating) AS total_ratings
FROM product_reviews
GROUP BY product_id
HAVING AVG(rating) > 4.5 AND COUNT(rating) > 10;
```

### **Example 2: Null-Safety in Aggregate Functions**
Scenario: Calculate the total sales for each product, ensuring that sales with null values are treated as 0.

```
SELECT
  product_id,
  SUM(COALESCE(sales_amount, 0)) AS total_sales
FROM sales_data
GROUP BY product_id;
```

### **Example 3: Grouping by Calculated Fields**
Scenario: Group sales data by quarter and year, then identify quarters with total sales exceeding $100,000.

```
SELECT
  EXTRACT(YEAR FROM sale_date) AS sale_year,
  EXTRACT(QUARTER FROM sale_date) AS sale_quarter,
  SUM(sales_amount) AS total_sales
FROM sales_data
GROUP BY sale_year, sale_quarter
HAVING SUM(sales_amount) > 100000;
```

### **Example 4: Handling Complex Grouping and Aggregation**

```
SELECT
  department_id,
  AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id
HAVING COUNT(*) > 5;
```

# 11. CTE and Recursive usage

### **Example 1: Employee and Department Performance Analysis**
Question: How can you find the average salary of employees in each department and list departments with an average salary above a certain threshold, say $50,000?

```
WITH department_salary AS (
  SELECT
    department_id,
    AVG(salary) AS avg_salary
  FROM employees
  GROUP BY department_id
)
SELECT
  department_id,
  avg_salary
FROM department_salary
WHERE avg_salary > 50000;
```

### **Example 2: Hierarchical Data Traversal**
Question: How can you retrieve a list of employees and their managers, assuming the employees table has a self-referencing manager_id column?

```
WITH RECURSIVE employee_hierarchy AS (
  SELECT
    employee_id,
    name,
    manager_id
  FROM employees
  WHERE manager_id IS NULL -- Assuming top-level managers have a NULL manager_id
  UNION ALL
  SELECT
    e.employee_id,
    e.name,
    e.manager_id
  FROM employees e
  INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT
  eh.name AS employee_name,
  m.name AS manager_name
FROM employee_hierarchy eh
LEFT JOIN employees m ON eh.manager_id = m.employee_id;
```



## Business/schema categories

**1. Ecommerce:**
1. Products
2. Orders
3. Order Items
4. Customers
5. Cart Items
6. Product Reviews
7. Product Categories
8. Payment Transactions
9. Shipping Methods
10. Inventory

**2. Fintech and Financial Services:**
1. Accounts
2. Transactions
3. Loans
4. Payment Methods
5. Investment Portfolios
6. Credit Cards
7. Customer Profiles
8. Expense Reports
9. Account Balances
10. Currency Exchange Rates

**3. Digital Marketing and Website Analytics:**
1. Page Views
2. User Sessions
3. Events
4. Conversions
5. Ad Clicks
6. Sources
7. Landing Pages
8. Geographies
9. Campaign Performance
10. Email Campaigns

**4. Marketplace (General and Niche):**
1. Listings
2. Users
3. Transactions
4. Reviews
5. Favorites
6. Categories
7. Messages
8. Bids
9. Shipping Options
10. Payment Logs

**5. SaaS (Software as a Service):**
1. Users
2. Subscriptions
3. Activity Logs
4. Features Accessed
5. Invoices
6. User Settings
7. Permissions
8. Service Integrations
9. Application Errors
10. Billing Info

**6. Delivery and Logistics:**
1. Orders
2. Deliveries
3. Routes
4. Vehicles
5. Drivers
6. Tracking Events
7. Warehouses
8. Inventory Transfers
9. Delivery Exceptions
10. Customer Addresses

**7. Content and Digital Media:**
1. Articles
2. Users
3. Comments
4. Categories
5. Media Assets
6. Subscriptions
7. Authors
8. Views Stats
9. Advertising Slots
10. Likes/Favorites

**8. Social Networks and Platforms:**
1. Users
2. Posts
3. Comments
4. Friendships
5. Groups
6. Messages
7. Notifications
8. Page Follows
9. User Activities
10. Privacy Settings

**9. EdTech and Online Education:**
1. Courses
2. Enrollments
3. Students
4. Assignments
5. Quiz Attempts
6. Discussion Posts
7. Teacher Profiles
8. Course Materials
9. Progress Tracks
10. Certificates Earned

**10. Automotive and Transportation:**
1. Vehicles
2. Reservations
3. Fleet Maintenance Records
4. Driver Logs
5. Trip Histories
6. Fuel Logs
7. Parts Inventory
8. Customer Feedback
9. Insurance Records
10. Route Optimizations

**11. Real Estate and Property Management:**
1. Properties
2. Leases
3. Tenants
4. Maintenance Requests
5. Property Listings
6. Viewings Appointments
7. Financial Transactions
8. Owners
9. Building Amenities
10. Insurance Policies

**12. Gaming and eSports:**
1. Users
2. Games
3. Matches
4. Player Stats
5. Teams
6. Tournaments
7. Leaderboards
8. In-game Purchases
9. Game Servers
10. Event Logs

**13. Analytics and Data Services:**
1. Datasets
2. Data Queries
3. User Reports
4. Analytics Models
5. Execution Logs
6. Data Sources
7. Visualization Settings
8. User Preferences
9. Scheduled Tasks
10. Access Permissions

**14. Insurance Tech:**
1. Policies
2. Claims
3. Policy Holders
4. Risk Assessments
5. Insurance Products
6. Agent Profiles
7. Coverage Options
8. Payment Histories
9. Claim Adjustments
10. Customer Inquiries

**15. Charities and Non-profits:**
1. Donors
2. Donations
3. Projects
4. Beneficiaries
5. Volunteer Profiles
6. Events
7. Fundraising Campaigns
8. Grants
9. Financial Reports
10. Feedback Surveys
