SELECT * FROM weatherfinal;
-- 1. Give the count of the minimum number of days for the time when temperature reduced
CREATE TEMPORARY TABLE cmin_days
SELECT temperature, upd_date, month, year, day,
LEAD(temperature,1) OVER (  PARTITION BY year ORDER BY upd_date ) AS temperature_leadone
FROM
weatherfinal;
SELECT count(oneday) AS count_min_oneday_temp_reduce 
FROM
(
SELECT temperature,
temperature_leadone,
CASE WHEN temperature>temperature_leadone THEN 1 END AS oneday
from 
cmin_days
) AS ct;


-- 2. Find the temperature as Cold / hot by using the case and avg of values of the given data set
SELECT maximum_temperature_f,minimum_temperature_f,
ROUND((maximum_temperature_F + minimum_temperature_f)/2,2) AS avg_temperature,
CASE WHEN (maximum_temperature_F + minimum_temperature_F)/2  >65 THEN 'hot' ELSE 'cold' END AS hot_cold
FROM
weatherfinal
ORDER BY avg_temperature;


-- 3. Can you check for all 4 consecutive days when the temperature was below 30 Fahrenheit
CREATE TEMPORARY TABLE less_than_thirty
SELECT 
temperature , upd_date,
month, year, day
FROM
weatherfinal
WHERE temperature <30;
SELECT *  FROM less_than_thirty;

SELECT temperature, upd_date,
LEAD(upd_date,1) OVER ( PARTITION BY year ORDER BY upd_date ) AS first_day, 
LEAD(upd_date,2) OVER ( PARTITION BY year ORDER BY upd_date ) AS second_day,
LEAD(upd_date,3) OVER ( PARTITION BY year ORDER BY upd_date ) AS third_day
FROM less_than_thirty;
SELECT
temperature,upd_date,first_day,second_day,third_day
FROM(
SELECT temperature, upd_date,
LEAD(upd_date,1) OVER (  PARTITION BY year ORDER BY upd_date ) AS first_day,
LEAD(upd_date,2) OVER (  PARTITION BY year ORDER BY upd_date ) AS second_day,
LEAD(upd_date,3) OVER (  PARTITION BY year ORDER BY upd_date ) AS third_day
FROM lessthan_thirty) AS dt
WHERE third_day=date_add(upd_date , INTERVAL 3 DAY);

-- 4. Can you find the maximum number of days for which temperature dropped
 CREATE TEMPORARY TABLE  max_days
SELECT temperature, upd_date, month, year, day,
LEAD(temperature,1) OVER (  PARTITION BY year ORDER BY upd_date ) AS temperature_leadone,
LEAD(temperature,2) OVER (  PARTITION BY year ORDER BY upd_date ) AS temperature_leadtwo,
LEAD(temperature,3) OVER (  PARTITION BY year ORDER BY upd_date ) AS temperature_leadthree,
LEAD(temperature,4) OVER (  PARTITION BY year ORDER BY upd_date ) AS temperature_leadfour,
LEAD(temperature,5) OVER (  PARTITION BY year ORDER BY upd_date ) AS temperature_leadfive,
LEAD(temperature,6) OVER (  PARTITION BY year ORDER BY upd_date ) AS temperature_leadfsix,
LEAD(temperature,7) OVER (  PARTITION BY year ORDER BY upd_date ) AS temperature_leadseven,
LEAD(temperature,8) OVER (  PARTITION BY year ORDER BY upd_date ) AS temperature_leadeight
FROM
weatherfinal;
SELECT * FROM max_days;
SELECT COUNT(oneday) AS oneday_dropcount,
COUNT(twodays) AS twoday_dropcount,
COUNT(threedays) AS threeday_dropcount,
COUNT(fourdays) AS fourday_dropcount,
COUNT(fivedays) AS fiveday_dropcount,
COUNT(sixdays) AS sixday_dropcount,
COUNT(sevendays)AS sevenday_dropcount,
COUNT(eightdays) AS eightday_dropcount

FROM
(
SELECT temperature,
temperature_leadone,temperature_leadtwo,
temperature_leadthree,temperature_leadfour,
CASE WHEN temperature>temperature_leadone THEN 1 END AS oneday,
CASE WHEN temperature>temperature_leadone AND temperature_leadone > temperature_leadtwo THEN 2 END AS twodays,
CASE WHEN temperature>temperature_leadone AND temperature_leadone > temperature_leadtwo AND temperature_leadtwo >temperature_leadthree THEN 3 END AS threedays,
CASE WHEN temperature>temperature_leadone AND temperature_leadone > temperature_leadtwo AND temperature_leadtwo >temperature_leadthree 
 AND temperature_leadthree >temperature_leadfour 
THEN 4 END AS fourdays,
CASE WHEN temperature>temperature_leadone AND temperature_leadone > temperature_leadtwo AND temperature_leadtwo >temperature_leadthree  AND temperature_leadthree >temperature_leadfour 
AND temperature_leadfour > temperature_leadfive
THEN 5 END AS fivedays,
CASE WHEN   temperature>temperature_leadone AND temperature_leadone > temperature_leadtwo AND temperature_leadtwo >temperature_leadthree  AND temperature_leadthree >temperature_leadfour 
AND temperature_leadfour > temperature_leadfive AND temperature_leadfive >temperature_leadfsix
THEN 6 END AS sixdays,
CASE WHEN   temperature>temperature_leadone and temperature_leadone > temperature_leadtwo AND temperature_leadtwo >temperature_leadthree  AND temperature_leadthree >temperature_leadfour 
AND temperature_leadfour > temperature_leadfive and temperature_leadfive >temperature_leadfsix AND temperature_leadfsix >temperature_leadseven
THEN 7 END AS sevendays,
CASE WHEN   temperature>temperature_leadone and temperature_leadone > temperature_leadtwo AND temperature_leadtwo >temperature_leadthree  AND temperature_leadthree >temperature_leadfour 
AND temperature_leadfour > temperature_leadfive and temperature_leadfive >temperature_leadfsix AND temperature_leadfsix >temperature_leadseven and temperature_leadseven >temperature_leadeight
THEN 8 END AS eightdays
FROM
max_days
) AS cmax;



-- 5. Can you find the average of average humidity from the dataset 
-- ( NOTE: should contain the following clauses: group by, order by, date )
SELECT
month,year,
ROUND(AVG (average_humidity_p),2) AS avg_humidity
FROM weatherfinal
GROUP BY month,year
ORDER BY AVG(average_humidity_p) DESC;

-- 6. Use the GROUP BY clause on the Date column and make a query to fetch details for average windspeed ( which is now windspeed done in task 3 )
SELECT   month,day,
ROUND(AVG(maximum_windspeed_mph),2) AS  avg_windspeed
FROM
weatherfinal
GROUP BY month,day;



-- 8. If the maximum gust speed increases from 55mph, fetch the details for the next 4 days
CREATE TEMPORARY TABLE gust_speed_from_fiftyfive
SELECT
maximum_gust_speed_mph , upd_date,month,year,day
FROM
weatherfinal
WHERE maximum_gust_speed_mph >55;
SELECT * from gust_speed_from_fiftyfive;


CREATE TEMPORARY TABLE first_following_day
SELECT *
FROM weatherfinal
WHERE upd_date IN 
(
SELECT DATE_ADD(upd_date , INTERVAL 1 DAY) AS d1 
FROM
gust_speed_from_fiftyfive
) ;


CREATE TEMPORARY TABLE second_following_day
SELECT *
FROM weatherfinal
WHERE upd_date IN 
(
SELECT date_add(upd_date , INTERVAL 2 DAY) AS d2
FROM
gust_speed_from_fiftyfive
) ;

CREATE TEMPORARY TABLE third_following_day
SELECT *
FROM weatherfinal
WHERE upd_date IN 
(
SELECT date_add(upd_date , INTERVAL 3 DAY) AS d3
FROM
gust_speed_from_fiftyfive
) ;

CREATE TEMPORARY TABLE fourth_following_day
SELECT *
FROM weatherfinal
WHERE upd_date IN 
(
SELECT date_add(upd_date , INTERVAL 4 DAY) AS d4
from
gust_speed_from_fiftyfive
) ;

SELECT * FROM first_following_day
UNION
SELECT * FROM second_following_day
UNION
SELECT * FROM third_following_day
UNION
SELECT * FROM fourth_following_day
ORDER BY  upd_date ;



-- 9. Find the number of days when the temperature went below 0 degrees Celsius 
SELECT temperature,upd_date
FROM weatherfinal
WHERE temperature < 32;
SELECT count(temperature)
FROM weatherfinal
WHERE temperature < 32;




 

