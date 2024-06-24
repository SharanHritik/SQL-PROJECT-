create database railway

use railway

Create table Railway_Table (TransactionID varchar(max),	DateofPurchase varchar(max), TimeofPurchase varchar(max), PurchaseType varchar(max), PaymentMethod varchar(max)
, Railcard varchar(max), TicketClass varchar(max), TicketType	varchar(max), Price varchar(max), DepartureStation varchar(max), ArrivalDestination varchar(max)
, DateofJourney varchar(max), DepartureTime varchar(max), ArrivalTime varchar(max), ActualArrivalTime varchar(max), JourneyStatus varchar(max), ReasonforDelay varchar(max)
, RefundRequest varchar(max));

Bulk insert Railway_Table
from 'C:\Users\Sharan Hritik\Downloads\railway 1.csv'
with (fieldterminator= ',', rowterminator='\n', firstrow= 2,  MAXERRORS = 40)

select * from Railway_Table

select column_name, data_type
from information_schema.columns



alter table Railway_Table
alter column TimeofPurchase  time 
-------------------------------------------------------------
--- as per the cleaning data we found that date and time are not in the default manner so we will be creating a function to update the data to its default manner.
--- Lets create a function for Date updates



 create function dbo.cnvrtdate(@inputDate varchar(max))
returns date
as
begin
    declare @cnvrtdate date
	declare @intermediatedate varchar(max)

    if patindex('%[^0-9]%', @inputDate) > 0
    begin
       
        set @intermediatedate = replace(@inputdate, '--', '-')
        set @intermediatedate = replace(@intermediatedate, '/', '-')
        set @intermediatedate = replace(@intermediatedate, '%', '-')
        set @intermediatedate = replace(@intermediatedate, '*', '-')
        set @intermediatedate = replace(@intermediatedate, '.', '-')
        set @intermediatedate = replace(@intermediatedate, ' ', '-')
    end

 
    if patindex('%[^0-9-]%', @intermediatedate) > 0
    begin
        return null
    end
  set @cnvrtdate = try_convert(date, @intermediatedate, 103) 
    if @cnvrtdate is null
    begin
     set @cnvrtdate = try_convert(date, @intermediatedate, 121) 
        if @cnvrtdate is null
  begin
  set @cnvrtdate = try_convert(date, @intermediatedate, 101)
            if @cnvrtdate is null
            begin
     set @cnvrtdate = try_convert(date, @intermediatedate, 102)
    end
    end
    end

    return @cnvrtdate
end


--- since we created a function we are going to update all the date by using it

select dbo.cnvrtdate (DateofPurchase) as cleandate
from Railway_Table

update Railway_Table
set DateofPurchase = dbo.cnvrtdate(DateofPurchase)


alter table Railway_Table
alter column DateofPurchase Date
-----------------------------------------------------------------------


select * from Railway_Table

-----------------------------------------------------------------------------------------------------------
select dbo.cnvrtdate (DateofJourney) as cleandate
from Railway_Table

update Railway_Table
set DateofJourney =  dbo.cnvrtdate (DateofJourney)

alter table Railway_Table
alter column DateofJourney Date

select * from Railway_Table
----------------------------------------------------------------------------------------------------------
--- since we cleared all the date data now we are going to create function for Time type data

CREATE FUNCTION  dbo.ConvTIME(@TimeString VARCHAR(MAX))
RETURNS TIME
AS
BEGIN
    DECLARE @CleanedTime VARCHAR(8);

    -- Remove any unwanted characters and ensure proper format (e.g., HH:MM:SS)
    SET @CleanedTime = REPLACE(@TimeString, '.', ':'); -- Replace dots with colons
    SET @CleanedTime = REPLACE(@CleanedTime, '-', ':'); -- Replace dashes with colons
    SET @CleanedTime = REPLACE(@CleanedTime, '/', ':'); -- Replace slashes with colons

    -- Convert to time data type
    DECLARE @ConvertedTime TIME;
    SET @ConvertedTime = TRY_CONVERT(TIME, @CleanedTime);

    RETURN @ConvertedTime;
END;




--- Since we got the function to clean time type data now we are going to convert it one by one

select  dbo.ConvTIME(TimeofPurchase) as cleandate
from Railway_Table

update Railway_Table 
set TimeofPurchase = dbo.ConvTIME(TimeofPurchase)

alter table Railway_table
alter column TimeofPurchase time

select *from Railway_Table

--------------------------------------------------------------------------------------------------------
select  dbo.ConvTIME(DepartureTime) as cleandate
from Railway_Table

update Railway_Table 
set DepartureTime = dbo.convrtTime(DepartureTime)

alter table Railway_table
alter column DepartureTime time


select *from Railway_Table
------------------------------------------------------------------------------------------------------------
select  dbo.ConvTIME(ArrivalTime) as cleandate
from Railway_Table


update Railway_Table 
set ArrivalTime = dbo.ConvTIME(ArrivalTime)

alter table Railway_table
alter column ArrivalTime time

select *from Railway_Table
------------------------------------------------------------------------------------------------------------
select dbo.ConvTIME(ActualArrivalTime) as cleandate
from Railway_Table

update Railway_Table
set ActualArrivalTime = dbo.ConvTIME(ActualArrivalTime)
where ActualArrivalTime is not null

alter table Railway_table
alter column ActualArrivalTime time

select *from Railway_Table

--------------------------------------------------------------------------------------------------------------
--- Since date and time had more than one column so we have used function to clean it as other column we can clean manualy

select price from Railway_Table
where ISNUMERIC(price) = 0

select Price from Railway_Table
where Price like '%[!@#$%^&*()_+:";<>.,^£]%' or price like '%[--]%'

select price 
from Railway_Table 
where patindex('%[^0-9.]%', price) > 0

update Railway_Table
set price = replace(replace(replace(replace(replace(replace
               (price, '&^', ''), '$', ''), '--', ''), '%', ''), 'ú', ''), 'A', '')
where price like '%&^%' or price like '%$%' or price like '%--%' 
         or price like '%ú%' or price like '%A%'

alter table Railway_Table
alter column Price int

select *from Railway_Table

alter table Railway_Table
alter column TransactionID varchar(max)

select *from Railway_Table

---- Since we have cleaned all the data now we will recheck if all are cleaned or any duplicates are there or not

select column_name, data_type
from information_schema.columns

--- Since all are cleaned
--- Now we will check Duplicates 

select TransactionID from Railway_Table
select count(distinct TransactionID) as uniq_id from Railway_Table

-----Since as a conclusion we cannot find any duplicates in the column
----- Now we will check all rows for overall cleaning 

with Duplrows as
(select *, row_number() over(partition by 
TransactionID order by DateofPurchase asc ) as row_num
from Railway_Table)
select * from Duplrows
where row_num>1

----Hence after cleaning there are no duplicates found 
--- Now we will head forward and analyse the query given

select * from Railway_Table

--- So in the first analysis we are going to analyse the peak purchase time and the impact on the delays.
----- So for that we are goiing to firstly analyse peak puchase Date and then peak purchase time.
------------------------------------------------------------------------------------------------------------------
--- Peak purchase Date-----

Select  DATEPART(HOUR, CAST(TimeofPurchase AS TIME)) AS PurchaseHour,
COUNT(*) AS PurchaseCount
from Railway_Table
group by DATEPART(HOUR, CAST(TimeofPurchase AS TIME))
order by PurchaseCount DESC;

---- Peak purchase Time-----

Select  CAST(DateofPurchase AS DATE) AS PurchaseDate, 
COUNT(*) AS PurchaseCount
from Railway_Table
group by CAST(DateofPurchase AS DATE)
order by PurchaseCount DESC;

--- Now we are going to analyse The delays which will be done by compairing ActualArrivalTime with ArrivalTime-----

select TransactionID,DepartureStation,
    ArrivalDestination,
    CAST(ArrivalTime AS TIME) AS ScheduledArrival,
    CAST(ActualArrivalTime AS TIME) AS ActualArrival,
    CASE 
        WHEN CAST(ActualArrivalTime AS TIME) > CAST(ArrivalTime AS TIME) THEN 1
        ELSE 0
    END AS IsDelayed
from Railway_Table

-----Now we are going to calculates the delay rate for each purchase hour, identifying any correlation between purchase times and delays------


;with PurchaseHours AS (SELECT 
        DATEPART(HOUR, CAST(TimeofPurchase AS TIME)) AS PurchaseHour,
        COUNT(*) AS PurchaseCount
    FROM Railway_Table
    GROUP BY DATEPART(HOUR, CAST(TimeofPurchase AS TIME))
),
DelayedJourneys AS (
    SELECT 
        DATEPART(HOUR, CAST(TimeofPurchase AS TIME)) AS PurchaseHour,
        COUNT(*) AS DelayCount
    FROM Railway_Table
    WHERE CAST(ActualArrivalTime AS TIME) > CAST(ArrivalTime AS TIME)
    GROUP BY DATEPART(HOUR, CAST(TimeofPurchase AS TIME))
)select 
    p.PurchaseHour,
    p.PurchaseCount,
    ISNULL(d.DelayCount, 0) AS DelayCount,
    ISNULL(CAST(d.DelayCount AS FLOAT) / p.PurchaseCount, 0) AS DelayRate
FROM PurchaseHours p
left join DelayedJourneys d ON p.PurchaseHour = d.PurchaseHour
ORDER BY DelayRate DESC;

----So after the analysis we found 24 records out of that we have 21 records where Impact of delay can be seen a
---- where in last 3 records we can see that there is no impact on delay but purchase hour and purchase count has also redused

select *from Railway_Table

---- As per the 2nd analysis where wwe need to find out out of all the records who are the one who are travelling frequently that is 
----who all are the one who are traveling more than 3 time.

SELECT TransactionID, COUNT(*) AS NumberOfJourneys
FROM Railway_Table 
GROUP BY TransactionID
HAVING COUNT(*) > 3;
---- since we are tried analysing by the help of transactionID we could not find out the count so we are going to try diffrent methods as well

------ Fisrtly lets check which are the most popular departure and arrival station and get the most travelled count

SELECT DepartureStation, ArrivalDestination, COUNT(*) AS JourneyCount
FROM Railway_Table
GROUP BY DepartureStation, ArrivalDestination
ORDER BY JourneyCount DESC;

--- after analysing popular ARRIVAL and DEPARTURE station we have found out the journy Count where we can see that most journy has happend between
---1st  MANCHESTER PICCADILLY AND LIVERPOOL LIME STREET ,2nd LONDON EUSTON AND BIRMINGHAM NEW STREET, 3RD LANDON KING CROSS AND YORK
---- Now lets check Popular travel Time

SELECT DateofJourney, COUNT(*) AS JourneyCount
FROM Railway_Table
GROUP BY DateofJourney
ORDER BY JourneyCount DESC;

--- After the analysis we got that the 3  most popular date of journey was on
--- 2024-03-09 with 313 jounry count
--- 2024-04-19 wuth 304 journy count
--- 2024-03-25 with 304 journy count

--- Now we are going to analyse the ticket type so that we get to know what kind of ticket are taken most 

SELECT TicketType, COUNT(*) AS TicketCount
FROM Railway_Table 
GROUP BY TicketType
ORDER BY TicketCount DESC;

--- Hence after the analysis we got to know that there aer 3 typw of ticket are sold
--- where Advance have the highest followe by OFF-PEAK and then ANYTIME.

--- Now we are going to analyse Delay and Refund request so that we can get how many have refunded due to delay 

SELECT ReasonforDelay, COUNT(*) AS DelayCount
FROM Railway_Table
WHERE ReasonforDelay IS NOT NULL
GROUP BY ReasonforDelay
ORDER BY DelayCount DESC;

SELECT COUNT(*) AS RefundRequests
FROM Railway_Table
WHERE RefundRequest = 'Yes';

--- Hence After the evaluation we got that 
--- REFUND REQUEST = 1118
--- Where we have 7 reason for delay in which WEATHER have the most delays which is equal to 995
--- then Signal Failure with 970 , Technical Issues with 707 , Staffing with 410, Staff Shortage with 399
--- Weather conditions with 377 and Traffic with 314

---- Now at with the last analysis we can find out payment methods which will help us to find the way of buying ticket 

SELECT PaymentMethod, COUNT(*) AS PaymentCount
FROM Railway_Table
GROUP BY PaymentMethod
ORDER BY PaymentCount DESC;

--- Hence we can see that only 3 types of payment are there where 
--- 1st in credit card which is used mostly with payment of 19136
--- then contactless with payment of 10834 and then debit card with 1683
 
--- Hence after all these query we can say that Maximum tickets are sold from the partern where top three are
--- MANCHESTER PICCADILY TO LIVERPOOL LIME STREET 
--- LONDON EUSTON TO BIRMINGHAM NEW STREET 
--- LODON KINGS CORSS TO YORK 
--- Since We want to know the more than three time we can find it out by from these three routs.

------- Now moving on to the next analysis where we have to find out the total revenue losss Due to Delays with Refund Requests

SELECT SUM(CAST(Price AS DECIMAL(18,2))) AS TotalRevenueLoss
FROM Railway_Table
WHERE ReasonforDelay IS NOT NULL
AND RefundRequest = 'Yes';

--- Hence we find out that 38702.00 is the total loss accured due to Delay and Refund Request 

--- Now going forward with other analysis where average ticket price and delay rate for journeys purchased with and without railcards.

SELECT
    Railcard,
      AVG(CAST(Price AS DECIMAL(18,2))) AS AvgTicketPrice,
      COUNT(*) AS JourneyCount,
    SUM(CASE WHEN ReasonforDelay IS NOT NULL THEN 1 ELSE 0 END) AS DelayedJourneys,
    CAST(SUM(CASE WHEN ReasonforDelay IS NOT NULL THEN 1 ELSE 0 END) AS DECIMAL(18,2)) / COUNT(*) * 100 AS DelayRate
FROM
    Railway_Table
GROUP BY
    Railcard;

--- Hence how we analysed it 
---We select the Railcard column to distinguish journeys made with different types of railcards.
---We calculate the average ticket price (AvgTicketPrice) for each railcard type using the AVG() function, casting the Price column to DECIMAL to ensure accurate arithmetic.
---We count the total number of journeys (JourneyCount) for each railcard type.
---We sum up the number of delayed journeys (DelayedJourneys) by using a conditional SUM() function, counting 1 for each delayed journey where the ReasonforDelay column is not null.
---We calculate the delay rate (DelayRate) by dividing the count of delayed journeys by the total number of journeys, multiplying by 100 to get the percentage.

--- After analysing we found out that there are 4 delay rate with or without railcard
--- where in 1st without railcard we had total of 12.821 with avgticket price of 27.425
---  and where in with we have 3 
--- Disabled have Delay rate of 10.553 where avgticket price of 10.553 
--- Senior have delay rate of 9.928 where avgticket price of 10.577
--- Adult have delay rate of 18.283 where avgticket price of 17.814

--- Moving forward with the analysis we are going to evaluate the performance of journeys by calculating the average delay time for each pair of departure and arrival stations.

Select * from Railway_Table

SELECT 
    DepartureStation, 
    ArrivalDestination, 
    COUNT(*) AS TotalJourneys,
    SUM(CASE WHEN ReasonforDelay IS NOT NULL THEN 1 ELSE 0 END) AS DelayedJourneys,
    AVG(CASE WHEN ReasonforDelay IS NOT NULL THEN DATEDIFF(minute, TRY_CONVERT(datetime, DepartureTime), TRY_CONVERT(datetime, ActualArrivalTime)) ELSE 0 END) AS AvgDelayTime
FROM 
    Railway_Table
GROUP BY 
    DepartureStation, 
    ArrivalDestination;

--- Hence after analysis we found out that after pairing the DepartureStation and ArrivalDestination we have 65 pairs where we got TotalJourneys ,DelayedJurneys and AgDelaysTime .

--- Now going forward we are going to analyse with delay statistics, providing insights into journeys' performance and revenue impact involving different railcards and stations.

WITH RevenueDelayAnalysis AS (
    SELECT
 Railcard,
        DepartureStation,
 COUNT(TransactionID) AS TotalJourneys,
 SUM(CAST(Price AS DECIMAL(10, 2))) AS TotalRevenue,
 SUM(CASE WHEN JourneyStatus = 'Delayed' THEN 1 ELSE 0 END) AS TotalDelays
 FROM
      Railway_Table
    GROUP BY
        Railcard,
        DepartureStation)
SELECT
    Railcard,
    DepartureStation,
    TotalJourneys,
    TotalRevenue,
    TotalDelays,
    CASE 
        WHEN TotalJourneys > 0 THEN (TotalDelays * 100.0) / TotalJourneys 
        ELSE 0 
    END AS DelayPercentage
FROM
    RevenueDelayAnalysis
ORDER BY
    TotalRevenue DESC;

--- Hence after the analysis we found at that there are 39 Departure station in which delay was impacted were we can see that there is no major
--- impact on revnue although we can say that slight changes are there.

--- Coming up with the next analysis how delays vary across different hours of the day, calculating the average delay in minutes for each hour and identifying the peak hours for delays.


WITH HourlyDelayAnalysis AS (
    SELECT
        DATEPART(HOUR, CONVERT(DATETIME, DepartureTime)) AS DepartureHour,
        AVG(DATEDIFF(MINUTE, CONVERT(DATETIME, DepartureTime), CONVERT(DATETIME, ActualArrivalTime))) AS AvgDelayMinutes
    FROM
        Railway_Table
    WHERE
        JourneyStatus = 'Delayed'
        AND CONVERT(DATETIME, DepartureTime) IS NOT NULL
        AND CONVERT(DATETIME, ActualArrivalTime) IS NOT NULL
    GROUP BY
        DATEPART(HOUR, CONVERT(DATETIME, DepartureTime))
)
SELECT
    DepartureHour,
    AvgDelayMinutes
FROM
    HourlyDelayAnalysis
ORDER BY
    AvgDelayMinutes DESC;

--- After the analysis we can find out that there are 18 records in which we can find out that 
--- Peak hours of delays are accuringg during the early morning time between 4 to 6 









































