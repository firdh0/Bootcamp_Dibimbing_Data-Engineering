# Airline Customer Loyalty Data Analysis

## Project Summary

This project aims to analyze customer activity and demographic data from an airline loyalty program. Using Apache Spark, we performed a series of processes ranging from data cleaning, transformation, analysis, to visualization to gain valuable business insights.

The main goal is to understand customer behavior, identify key segments, and discover patterns that can be used to improve business and marketing strategies.

---

## Analysis Process Flow (Pipeline)

This data pipeline is designed modularly and executed sequentially by `main.py`. The main stages are as follows:

1.  **Initialization (`SparkManager`)**: Creates and configures a Spark session that forms the foundation of the entire process.
2.  **Data Loading (`DataLoader`)**: Loads raw datasets (`.csv`) from the `data/raw/` directory.
3.  **Data Cleaning (`DataCleaner`)**: Runs a series of cleaning processes on each dataset, including:
    * Standardizing column names.
    * Handling duplicate values.
    * Checking for and imputing null values using statistical methods (mean, median, or mode).
4.  **Clean Data Storage (`DataSaver`)**: Stores the results from the cleaning stage in Parquet format in the `data/cleaned/` directory as a checkpoint.
5.  **Data Transformation (`DataTransformer`)**: Joins flight activity data with customer demographic data based on `Loyalty_Number` to create a comprehensive dataset.
6.  **Data Analysis (`CustomerDataAnalyzer`)**: Executes a series of SQL queries on the merged data to answer key business questions. The results of each analysis are then saved in `.csv` format in the `data/analysis_results_csv/` directory.
7.  **Data Visualization (`DataVisualizer`)**: Converts analysis results into graphical visualizations (bar charts, line charts, scatter plots, etc.) and saves them as image files (`.png`) in the `final_visualizations/` directory.

---

## Analysis Details, Queries, and Interpretation

Below are the details of **all 14 analyses** performed, along with the SQL queries used, visualization results, and interpretation of potential insights found.

### 1. Average Flights per Customer per Year
* **Question:** What is the average number of flights for each customer per year?
* **SQL Query:**
    ```sql
    SELECT
        Loyalty_Number,
        Year,
        AVG(Total_Flights) as Avg_Flights_Per_Year
    FROM flight_activity_view
    GROUP BY Loyalty_Number, Year
    ORDER BY Loyalty_Number, Year
    ```

### 2. Points Distribution by Loyalty Card
* **Question:** How is the total points accumulated by customers distributed based on their loyalty card type?
* **SQL Query:**
    ```sql
    SELECT
        Loyalty_Card,
        SUM(Points_Accumulated) as Total_Points_Accumulated,
        AVG(Points_Accumulated) as Avg_Points_Accumulated_Per_Record
    FROM merged_view
    WHERE Loyalty_Card IS NOT NULL
    GROUP BY Loyalty_Card
    ORDER BY Total_Points_Accumulated DESC
    ```
* **Graph:**
    ![Graph of Points Distribution per Card](final_visualizations/points_by_card.png)
* **Insight Interpretation:**
    
    1. **Dominance of “Star ”** Cards: **Star** cardholders are the highest point contributors with a total accumulation of around **360 million points**. This indicates that this segment is the most active or has the highest spending value.

    2. **Clear hierarchy**: There are significant performance differences between card tiers:

        - **Rank 1: Star** (3.6 points)
        - **Rank 2: Nova** (2.7 points)
        - **Rank 3: Aurora** (1.6 points)

    3. **Indication of Customer Segmentation**: This sharp difference indicates effective customer segmentation. The “Star” card is most likely a premium tier card designed for high-value customers, while the “Aurora” could be a basic tier card.

* **Conclusion:**
    “Star” loyalty cardholders are the most valuable customer segment in the loyalty program, as evidenced by their points contribution that far exceeds other card types. The engagement of this segment is critical to the overall success of the program.

### 3. Relationship between Education and Number of Flights
* **Question:** Is there a relationship between a customer's education level and the average number of flights they take?
* **SQL Query:**
    ```sql
    SELECT
        Education,
        AVG(Total_Flights) as Avg_Total_Flights_Per_Record,
        SUM(Total_Flights) as Sum_Total_Flights
    FROM merged_view
    WHERE Education IS NOT NULL
    GROUP BY Education
    ORDER BY Avg_Total_Flights_Per_Record DESC
    ```
* **Graph:**
    ![Graph of Flights per Education](final_visualizations/flights_by_education.png)
* **Insight Interpretation:**
    1.  **No Significant Difference:** The main insight from this graph is that there is **no strong relationship or significant difference** between a customer's education level and the average number of flights they take. The heights of all the bars on the chart are almost the same.

    2.  **Nearly Uniform Average Values:** All education categories, ranging from “High School or Below” to “Doctor”, show very similar averages of around **1.25 to 1.3 flights**. The difference between the group with the highest (“High School or Below”) and lowest (“Doctor”) averages is very small and most likely not statistically or practically significant.

    3.  **Very Slight Downward Trend:** If you look very carefully, there is a very, very slight downward trend as the education level increases. Customers with an education level of “High School or Below” have the highest average flight, and it decreases marginally at the “College”, “Bachelor”, ‘Master’, and “Doctor” levels. However, again, this drop is so small that it is almost negligible.

* **Conclusion:**
    In conclusion, based on the data presented in this graph, a customer's education level is not a strong factor or predictor for determining their air travel frequency. Almost all customers, regardless of their educational background, take a similar number of flights on average.

### 4. Flight Trends Over Time
* **Question:** What is the trend of the total number of flights monthly over time?
* **SQL Query:**
    ```sql
    SELECT
        Year,
        Month,
        SUM(Total_Flights) as Total_Flights_Per_Month
    FROM flight_activity_view
    GROUP BY Year, Month
    ORDER BY Year, Month
    ```
* **Graph:**
    ![Graph of Monthly Flight Trends](final_visualizations/flight_trends.png)
* **Insight Interpretation:**
    1.  **Strong Seasonal Pattern:**
        * The trend in the number of flights does not show a consistent increase or decrease over time. Instead, it is heavily influenced by seasonal patterns that repeat every year.
        * **Peak Season:** The highest peak in the number of flights consistently occurs in the middle of the year, around July-August**. This was evident in 2017 and peaked even higher in 2018 (reaching over 37,500 flights). This peak most likely coincided with the summer holiday season.
        **Low Season:** The number of flights reaches its lowest point early in the year (around **January-February**) and in the fall (around **September-October**). These periods can be considered the “low season” for air travel.
        * **Secondary Peaks:** There are smaller peaks around the end of the year (December) and the beginning of the new year, which may be driven by the Christmas and New Year holidays.

    2.  **Year-over-Year Growth:**
        * While the pattern is similar, the peak flights in mid-2018 were significantly higher than the peak in mid-2017. This indicates a **growth in air travel demand** in the peak season from 2017 to 2018.

    3.  **Significant Fluctuations:**
        * The graph shows that there are drastic changes in the number of flights from month to month. For example, after peaking in the middle of the year, there is a sharp decline towards September and October. This shows that airlines and airports face huge fluctuations in demand throughout the year.

* **Conclusion:**
    Overall, the trend in the number of monthly flights is highly cyclical and seasonal. The main factor affecting flight volume is the vacation period, especially summer vacation which is the highest peak during the year.

### 5. Relationship between Salary and Flight Distance
* **Question:** Is there a relationship between a customer's `Salary` level and the `Distance` they typically travel?
* **SQL Query:**
    ```sql
    SELECT
        Salary,
        Distance
    FROM merged_view
    WHERE Salary IS NOT NULL AND Distance > 0
    LIMIT 5000
    ```
* **Graph:**
    ![Graph of Salary and Flight Distance Relationship](final_visualizations/salary_vs_distance.png)
* **Insight Interpretation:**
    1.  **No Clear Correlation:** The most prominent insight from this graph is that there is **no clear relationship or correlation** between the customer's salary level and the flight distance they traveled. The data points are scattered randomly and do not form a linear pattern (straight lines going up or down) or a clear curve pattern.

    2.  **Concentration of Data on Low-Medium Salary:** Most of the data is concentrated on the salary range below $100,000. Within this range, customers travel widely varying distances, ranging from very short flights (close to 0 miles) to long-haul flights (up to 40,000 miles or more).

    3.  **Unexpected Behavior:** Salary increases are not consistently followed by increases or decreases in flight distance. For example:
        * There is a customer with a salary of about $50,000 who flies 50,000 miles.
        * On the other hand, there are customers with very high salaries (close to $300,000) who only fly under 10,000 miles.
        * Vice versa, there is a customer with a high salary (around $380,000) who flies a very short distance.

* **Conclusion:**
    In conclusion, based on this visual data, there is no evidence to support the idea that people with higher salaries tend to fly farther or shorter.The relationship between these two variables is very weak or even non-existent.

### 6. Point Exchange Rate
* **Question:** What is the relationship between points redeemed (`Points_Redeemed`) and their monetary value in dollars (`Dollar_Cost_Points_Redeemed`)?
* **SQL Query:**
    ```sql
    SELECT
        Points_Redeemed,
        Dollar_Cost_Points_Redeemed
    FROM flight_activity_view
    WHERE Points_Redeemed > 0
    ```
* **Graph:**
    ![Graph of Point Exchange Rate](final_visualizations/points_exchange.png)
* **Insight Interpretation:**
    1.  **Perfect Positive Linear Relationship:** The main insight from this chart is that there is a very strong, if not perfect, positive linear relationship. The data points form a precise straight line from bottom left to top right.
    **Linear:** The relationship is straight, which means every addition of the same number of points will result in the same addition of dollar value.
    **Positive:** The relationship is positive, which means as the number of `Points Redeemed` increases, the `Cost in Dollars` also increases.

    2.  **Fixed Conversion Rate:** This density of points forming a perfectly straight line indicates that the relationship between points and dollars is not a statistical correlation found from random data, but rather a **fixed conversion rule or formula**. There is a fixed exchange rate between points and dollars.

    3.  **Estimation of Exchange Rate:** We can estimate the exchange rate from the graph. Let's take two points as an example:
        * When the exchanged points are about **300**, the dollar value is about **$52**.
        * When the points exchanged are about **900**, the dollar value is about **$160**.

        Calculation:
        * Dollar Change: $160 - $52 = $108
        * Change in Points: 900 - 300 = 600 points
        * Value per point: $108 / 600 points = **$0.18 per point**.

        This means every 1 point redeemed has a value equivalent to $0.18.

* **Conclusion:**
    In conclusion, the relationship between the number of points redeemed (`Points_Redeemed`) and their monetary value (`Dollar_Cost_Points_Redeemed`) is **deterministic and governed by a constant exchange rate.** This is not just a trend, but a definite business rule, as commonly found in loyalty programs or reward systems.

### 7. Salary Distribution by Education
* **Question:** How does the distribution of salary (`Salary`) compare among customers with different education levels (`Education`)?
* **SQL Query:**
    ```sql
    SELECT
        Education,
        Salary
    FROM merged_view
    WHERE Education IS NOT NULL AND Salary IS NOT NULL
    ```
* **Graph:**
    ![Graph of Salary Distribution by Education](final_visualizations/salary_dist_by_education.png)
* **Insight Interpretation:**
    1.  **Education Level Affects Salary:** A key insight is that **there is a clear relationship between education level and income level**. In general, the higher the level of formal education, the higher the median salary and overall earning potential.

    2.  **Comparison Between Groups:**
        - **Doctor:** This group shows the **highest and widest salary distribution**.
            - **Median Salary:** The highest among all groups, around $180,000.
            - **Spread:** The box (IQR) and whiskers are very wide, ranging from around $50,000 to nearly $400,000. This indicates very high salary variability at this level; some earn very high salaries, but others earn significantly less. There are also outliers with salaries above $400,000.
        - **Master's:** Has the second-highest median salary, around $100,000. The salary spread is much narrower than that of the Doctorate group, meaning that income at this level is more concentrated around the median value. This is a clear improvement from the Bachelor's level.
        - **Bachelor's Degree:** The median salary is around $75,000. What is interesting about this group is the presence of many *outliers* on the lower end, including negative values. This indicates that while the median is fairly good, a significant portion of Bachelor's Degree graduates have very low incomes or may even have debt (if negative values represent debt).
        - **High School or Below:** This group has a median salary slightly lower than the Bachelor's degree level (around $65,000–$70,000). However, the data distribution is narrower and has fewer outliers compared to the Bachelor's degree level.
        - **College:** This category shows a **data anomaly**. The graph for “College” is just a straight line without any boxes or whiskers. This means that all data points in this category have the exact same salary value (around \$75,000) or the data is very limited/corrupted. This makes it impossible to analyze the actual distribution for this group.

* **Conclusion:**
    In conclusion, educational attainment is an important factor influencing the distribution of customer income, with the following pattern:

    1.  **Income Hierarchy:** There is a clear income hierarchy: **Doctorate > Master's > Bachelor's ≈ High School**.
    2.  **Highest Income Potential:** Doctorate degree holders have the highest income potential, but also the greatest variability (risk).
    3.  **Educational Value Added:** Continuing education to the Master's degree level results in a significant increase in median salary compared to the Bachelor's degree level.

### 8. Overall Salary Distribution
* **Question:** What is the overall distribution of customer income (`Salary`) in this loyalty program?
* **SQL Query:**
    ```sql
    SELECT Salary
    FROM merged_view TABLESAMPLE (10 PERCENT)
    WHERE Salary IS NOT NULL
    ```
* **Result:** 10% sample of customer salary data, used to create a histogram.
* **Graph:**
    ![Graph of Overall Salary Distribution](final_visualizations/salary_distribution.png)
* **Insight Interpretation:**
    1.  **Right-Skewed Distribution:** The most important insight is that the customer salary distribution is **right-skewed (or positively skewed)**.
        - **Meaning:** Most customers are concentrated in the low to middle income range. Meanwhile, there are a small number of customers with very high incomes, which form a long “tail” on the right side of the graph.
        - **Implications:** Because this distribution is not symmetrical, the average (*mean*) salary will be higher than the median. The median is a better measure to describe the “typical customer” because it is not overly influenced by the extreme incomes of a few individuals.

    2.  **Majority Customer Concentration:** The highest peak of the histogram (mode) indicates that the most common salary range for customers is around **$75,000 to $80,000**. The vast majority of customers fall within the salary range below $125,000.

    3.  **Presence of High-Income Groups:** Although their numbers are small, the existence of customers with salaries above $150,000, $200,000, and up to $400,000 is evident. This group, despite its small size, may represent a highly valuable customer segment (high-value customers).

    4.  **Data Anomaly:** There is a small bar in the negative salary range. This is likely an anomaly or error in the data that requires further investigation, as salaries are generally not negative.

* **Conclusion:**
   In conclusion, the customer base of this loyalty program is **dominated by individuals with incomes in the low to middle segments**. The profile of the “typical customer” has a salary of around $75,000-$80,000.

### 9. Demographic Composition
* **Question:** What is the composition of marital status (`Marital_Status`) within each education category (`Education`)?
* **SQL Query:**
    ```sql
    SELECT
        Education,
        Marital_Status,
        COUNT(*) as Customer_Count_Per_Segment
    FROM merged_view
    WHERE Education IS NOT NULL AND Marital_Status IS NOT NULL
    GROUP BY Education, Marital_Status
    ORDER BY Education, Marital_Status
    ```
* **Graph:**
    ![Graph of Demographic Composition](final_visualizations/demographic_composition.png)
* **Insight Interpretation:**
    1.  **Different Patterns at Each Level of Education:** The main insight is that the composition of marital status **varies greatly** and is not uniform across all levels of education. Each educational group exhibits unique demographic patterns.

    2.  **Dominance of “Married” Status at the Bachelor's Level:**
    * The group with a **bachelor's** level of education is the largest customer segment in absolute terms.
        * Within this group, the number of customers who are **Married** is highly dominant, exceeding 160,000 people. This number far exceeds the combined total of single and divorced customers. This indicates that the company's largest market segment is married college graduates.

    3.  **College and Master's Levels:**
        * **College:** Unlike other groups, at the **College** level, the **Single** status is the most common, with over 50,000 customers, followed by those who are married. This may indicate that customers in this category tend to be younger.
        * **Master's:** This group shows the most unique pattern. The number of customers who are **Divorced** is slightly higher than those who are **Married**, though the difference is not significant. This is the only category where the “Divorced” status is the most common.

    4.  **Patterns for Other Groups:**
        * **Doctor** and **High School or Below:** Both of these groups follow a more “traditional” pattern like the Bachelor group, where the **Married** status is the most dominant, although with a much smaller total number of customers.

* **Conclusion:**
    In conclusion, there is no single answer for the composition of marital status across all education levels, as the patterns are highly diverse:

    1.  **Largest Segment:** Customers with a Bachelor's degree who are married constitute the single most dominant demographic segment in this customer base.
    2.  **Dominant Pattern:** Generally, for education levels of **Bachelor's, Doctorate, and High School or Below**, married customers are the majority group.
    3.  **Distinctive Pattern:**
    * College-level customers tend to be single.
        * Customers with a **Master's degree** show a unique composition with nearly equal numbers of **Divorced** and **Married** customers, making them the largest group.

### 10. Regional Activity
* **Question:** Which province recorded the highest total number of flights?
* **SQL Query:**
    ```sql
    SELECT
        Province,
        SUM(Total_Flights) as Total_Flights_Per_Province
    FROM merged_view
    WHERE Province IS NOT NULL
    GROUP BY Province
    ORDER BY Total_Flights_Per_Province DESC
    ```
* **Graph:**
    ![Graph of Regional Activity](final_visualizations/regional_activity.png)
* **Insight Interpretation:**
    1.  **Dominance of Three Major Provinces:** The graph clearly shows that air traffic is highly concentrated in three provinces: **Ontario, British Columbia, and Quebec**. The number of flights in these three provinces significantly exceeds that of all other provinces.

    2.  **Significant Gap:** There is a very large gap between the top three provinces and the other provinces.
        * **Ontario** is at the top with more than 160,000 flights.
        * **British Columbia** is in second place with around 135,000 flights.
        * **Quebec** is third with 100,000 flights.
        * The fourth-ranked province, **Alberta**, only recorded around 30,000 flights, showing a very drastic decline.

    3.  **Relationship with Population and Economic Centers:** This dominance is no coincidence. Ontario, British Columbia, and Quebec are the most populous provinces and major economic hubs in Canada. These provinces are home to the country's busiest international airports (e.g., Toronto Pearson International Airport in Ontario, Vancouver International Airport in British Columbia, and Montréal–Trudeau International Airport in Quebec).

* **Conclusion:**
    In conclusion, this graph illustrates that aviation activity in Canada is highly concentrated. The three main provinces—Ontario, British Columbia, and Quebec—serve as the main pillars of the country's air transportation network. This reflects their central role as population, business, and tourism hubs in Canada, while other provinces have much lower air traffic volumes.

### 11. Point Redemption Value per Region
* **Question:** What is the total dollar value of points redeemed for each province?
* **SQL Query:**
    ```sql
    SELECT
        Province,
        SUM(Dollar_Cost_Points_Redeemed) as Total_Redemption_Value
    FROM merged_view
    WHERE Province IS NOT NULL AND Dollar_Cost_Points_Redeemed > 0
    GROUP BY Province
    ORDER BY Total_Redemption_Value DESC
    ```
* **Graph:**
    ![Graph of Point Redemption Value per Region](final_visualizations/regional_redemption.png)
* **Insight Interpretation:**
    1.  **Consistent Dominance of Major Provinces:** Similar to flight data, redemption values are also heavily dominated by three major provinces: **Ontario, British Columbia, and Quebec**. These three provinces account for the majority of total redemption values.

    2.  **Direct Correlation with Flight Activity:** There is a very strong direct correlation between the total redemption value and the total number of flights in each province. The provinces with the highest flight volume are also the provinces with the highest point redemption value. This is an important insight that shows that where points are earned (through flights), that is also where points are redeemed.

    3.  **The Pareto Principle in Action:** This distribution reflects the **Pareto Principle (80/20 Rule)**, where a small portion of entities (in this case, 3 out of 11 provinces) generate the majority of the results (total redemption value). The combined value of Ontario, BC, and Quebec far exceeds that of the remaining eight provinces combined.

    4.  **Clear Market Gap:** There is a very sharp gap between the top four provinces (especially the top three) and the rest of the provinces. This indicates that the market for this loyalty program is highly concentrated and unevenly distributed across the country.

* **Conclusion:**
    In conclusion, the economic value of loyalty point redemption activity is highly concentrated in Ontario, British Columbia, and Quebec. This pattern directly reflects where the most flight activity occurs, which are also the population and economic centers of Canada. From a business perspective, these three provinces are the most crucial and valuable markets for this loyalty program.

### 12. Financial Demographics
* **Question:** What is the average salary of customers based on marital status?
* **SQL Query:**
    ```sql
    SELECT
        Marital_Status,
        AVG(Salary) as Average_Salary
    FROM merged_view
    WHERE Marital_Status IS NOT NULL AND Salary IS NOT NULL
    GROUP BY Marital_Status
    ORDER BY Average_Salary DESC
    ```
* **Graph:**
    ![Graph of Financial Demographics](final_visualizations/avg_salary_by_marital_status.png)
* **Insight Interpretation:**
    1.  **Divorced Customers Have the Highest Average Salary:** The most notable insight from this graph is that the **Divorced** customer group has the highest average salary among the three groups.

    2.  **Indirect Relationship with Age and Career:** This pattern is most likely not directly caused by marital status itself, but rather related to other demographic factors such as **age and career level**. Divorced customers may be older on average than the other two groups, so they have more work experience, which contributes to higher incomes.

    3.  **Single Customers with the Lowest Average Salary:** Conversely, the **Single** customer group has the lowest average salary. This makes sense if we assume that this group includes many younger individuals who may still be in the early stages of their careers.

    4.  **Significant Differences:** Although the differences are not extreme, there are clear and measurable differences. There is a difference of approximately $7,000 in average annual income between the highest-income group (Divorced) and the lowest-income group (Single), indicating a significant pattern.

* **Conclusion:**
    In conclusion, there is a clear difference in average customer income based on their marital status. The hierarchy is as follows:

    1.  **Divorced** (Highest)
    2.  **Married** (Medium)
    3.  **Single** (Lowest)

    This pattern likely reflects the different **life stages and career progress** among these demographic groups. This information can be useful for business purposes such as market segmentation, where marital status can be used as one of the variables to estimate purchasing power or customer profiles.

### 13. Customer Composition by Gender
* **Question:** What is the proportion of customers based on gender (`Gender`)?
* **SQL Query:**
    ```sql
    SELECT
        Gender,
        COUNT(DISTINCT Loyalty_Number) as Number_of_Customers
    FROM merged_view
    WHERE Gender IS NOT NULL
    GROUP BY Gender
    ```
* **Graph:** <br>
    ![Graph of Customer Composition by Gender](final_visualizations/gender_composition.png)
* **Insight Interpretation:**
    1.  **Very Balanced Distribution:** The most obvious and key insight from this graph is that the composition of customers by gender is **very balanced**, with an almost exact 50/50 split.

    2.  **Negligible Difference:** The difference between the proportion of female and male customers is only 0.4%. In the context of business analysis, such a small difference can be considered neither statistically nor practically significant.

    3.  **Universal (Gender-Neutral) Appeal:** This balance indicates that the product, service or loyalty program being analyzed is equally attractive to both genders**. There is no strong preference or bias towards either men or women in its customer base.

* **Conclusion:**
    In conclusion, the program has a near perfect and balanced **gender distribution of customers**. This is a positive indicator that the product or service is successfully reaching a broad market regardless of gender. From a strategic point of view, this means that companies do not need to create marketing campaigns that specifically target one gender. Instead, a marketing approach that is **universal and inclusive** is the most effective strategy and suits the existing audience composition.

### 14. Loyalty Tier Engagement
* **Question:** Which loyalty card type (`Loyalty_Card`) has members who collectively travel the furthest total distance (`Distance`)?
* **SQL Query:**
    ```sql
    SELECT
        Loyalty_Card,
        SUM(Distance) as Total_Distance_Flown
    FROM merged_view
    WHERE Loyalty_Card IS NOT NULL
    GROUP BY Loyalty_Card
    ORDER BY Total_Distance_Flown DESC
    ```
* **Graph:**
    ![Graph of Loyalty Tier Engagement](final_visualizations/tier_engagement_by_distance.png)
* **Insight Interpretation:**
    1.  **Clear Hierarchy Between Tiers:** The graph shows a very clear and significant hierarchy between the three loyalty card tiers. The total distance traveled decreases sequentially from highest to lowest tier: **Star > Nova > Aurora**.

    2.  **Substantial Value Differences:** The distance differences between tiers are substantial, highlighting the differences in travel behavior between customer segments.
        - **Star** card members collectively flew approximately **345 million miles**.
        - **Nova** card members flew approximately **260 million miles**.
        - **Aurora** card members flew approximately **160 million miles**.
        Star members traveled almost twice as much distance as Aurora members.

    3.  **Reflection of Successful Customer Segmentation:** This pattern is very natural and expected from a tiered loyalty program. It shows that the program's tier structure is **successful in segmenting customers** based on their travel volume and frequency. Customers in the highest tier (Star) have indeed proven to be the most frequent and longest flying group.

* **Conclusion:**
    In conclusion, members with **Star** tier loyalty cards are the largest and most significant contributor to the total flight distance in the program. From a business standpoint, this confirms that **Star** tier members are the most active and most valuable customer segment for the airline in terms of travel volume. Therefore, strategies to retain customers, provide exclusive benefits, and priority services for Star members are crucial, as they are the main drivers of flight activity among loyalty program members.