-- We have to fix data types on all datasets 

create table daily_activity
(Id float not null, ActivityDate datetime2(7), TotalSteps int, TotalDistance float, TrackerDistance float, LoggedActivitiesDistance float, VeryActiveDistance float, ModeratelyActiveDistance float, LightActiveDistance float,
SedentaryActiveDistance float, VeryActiveMinutes int, FairlyActiveMinutes int, LightlyActiveMinutes int, SedentaryMinutes int, Calories float);

create table daily_calories
(Id float not null, ActivityDay datetime2(7), Calories int); 

create table daily_intensities
(Id float not null, ActivityDay datetime2(7), SendentaryMinutes int, LightlyActiveMinutes int, FairlyActiveMinutes int, VeryActiveMinutes int, SedentaryMinutes float, LightActiveDistance float, ModeratelyActiveDistance float,
VeryActiveDistance float);

create table daily_steps
(Id float not null, ActivityDay datetime2(7), StepTotal int);

create table heartrate_seconds
(Id float not null, Time datetime2(7), Value int);

create table hourly_calories
(Id float not null, ActivityHour datetime2(7), Calories float);

create table hourly_steps
(Id float not null, ActivityHour datetime2(7), StepTotal int);

create table sleep_day
(Id float not null, SleepDay datetime2(7), TotalSleepRecords int, TotalMinutesAsleep int, TotalTimeInBed int);

create table weight_logInfo
(Id float not null, Date datetime2(7), WeightKg float, WeightPounds float, Fat int, BMI float, IsManualReport bit, LogId float);

-- Copy the data from the old table to the new table.

insert into daily_activity
select * from daily_activity_old

insert into daily_calories
select * from daily_calories_old

insert into daily_intensities
select * from daily_intensities_old

insert into daily_steps
select * from daily_steps_old

insert into heartrate_seconds
select * from heartrate_seconds_old

insert into hourly_calories
select * from hourly_calories_old

insert into hourly_steps
select * from hourly_steps_old

insert into sleep_day
select * from sleep_day_old

insert into weight_logInfo
select * from weight_logInfo_old

-- delete old table.

drop table daily_activity_old;
drop table daily_calories_old;
drop table daily_intensities_old;
drop table daily_steps_old;
drop table heartrate_seconds_old;
drop table hourly_calories_old;
drop table hourly_steps_old;
drop table sleep_day_old;
drop table weight_logInfo_old;

-- checking total number of unique users.

select COUNT(distinct Id) as num_of_users -- 33 users
from daily_activity;

select COUNT(distinct Id) as num_of_users -- 33 users
from daily_calories;

select COUNT(distinct Id) as num_of_users -- 33 users
from daily_intensities;

select COUNT(distinct Id) as num_of_users -- 33 users
from daily_steps;

select COUNT(distinct Id) as num_of_users -- 14 users
from heartrate_seconds;

select COUNT(distinct Id) as num_of_users -- 33 users
from hourly_calories;

select COUNT(distinct Id) as num_of_users -- 33 users
from hourly_steps;

select COUNT(distinct Id) as num_of_users -- 24 users
from sleep_day;

select COUNT(distinct Id) as num_of_users -- 8 users
from weight_logInfo;

-- Checking null in all datasets.

SELECT * FROM daily_activity
WHERE Id IS NULL or ActivityDate is null or TotalSteps is null or TotalDistance is null or TrackerDistance is null or LoggedActivitiesDistance is null or VeryActiveDistance is null or ModeratelyActiveDistance is null or
LightActiveDistance is null or SedentaryActiveDistance is null or VeryActiveMinutes is null or FairlyActiveMinutes is null or LightlyActiveMinutes is null or SedentaryMinutes is null or Calories is null;

select * from daily_calories
where Id is null or ActivityDay is null or Calories is null;

select * from daily_intensities
where Id is null or ActivityDay is null or SedentaryMinutes is null or LightlyActiveMinutes is null or FairlyActiveMinutes is null or VeryActiveMinutes is null or SedentaryMinutes is null or LightActiveDistance is null or
ModeratelyActiveDistance is null or VeryActiveDistance is null;

select * from daily_steps
where Id is null or ActivityDay is null or StepTotal is null;

select * from heartrate_seconds
where Id is null or Time is null or Value is null;

select * from hourly_calories
where Id is null or ActivityHour is null or Calories is null;

select * from hourly_steps
where Id is null or ActivityHour is null or StepTotal is null;

select * from sleep_day
where Id is null or SleepDay is null or TotalSleepRecords is null or TotalMinutesAsleep is null or TotalTimeInBed is null;

select * from weight_logInfo -- In the Fat column, 65 rows have nulls. So this table is not needed because users are limited around 8 users are there and 65 rows have null.
where Id is null or Date is null or WeightKg is null or WeightPounds is null or Fat is null or BMI is null or IsManualReport is null or LogId is null;

-- Check for duplicates

select Id, ActivityDate, COUNT(*) as numRows
from daily_activity
group by Id, ActivityDate
having COUNT(*) > 1;

select Id, ActivityDay, COUNT(*) as numRows
from daily_calories
group by Id, ActivityDay
having COUNT(*) > 1;

select Id, ActivityDay, COUNT(*) as numRows
from daily_intensities
group by Id, ActivityDay
having COUNT(*) > 1;

select Id, ActivityDay, COUNT(*) as numRows
from daily_steps
group by Id, ActivityDay
having COUNT(*) > 1;

select Id, Time, COUNT(*) as numRows
from heartrate_seconds
group by Id, Time
having COUNT(*) > 1;

select Id, ActivityHour, COUNT(*) as numRows
from hourly_calories
group by Id, ActivityHour
having COUNT(*) > 1;

select Id, ActivityHour, COUNT(*) as numRows
from hourly_steps
group by Id, ActivityHour
having COUNT(*) > 1;

select Id, SleepDay, COUNT(*) as numRows -- In sleep_day table 3 duplicates find.
from sleep_day
group by Id, SleepDay
having COUNT(*) > 1;

select * from sleep_day

-- delete duplicates

DELETE FROM sleep_day 
WHERE Id IN (
    SELECT Id 
    FROM (
        SELECT Id, SleepDay, ROW_NUMBER() OVER(PARTITION BY Id, SleepDay ORDER BY Id) AS RowNum 
        FROM sleep_day
    ) AS dups
    WHERE RowNum > 1
);

select s.Id,
		s.ActivityHour,
		s.StepTotal,
		c.Calories
into merge_df
from hourly_calories as c
inner join hourly_steps as s
on c.Id = s.Id and c.ActivityHour = s.ActivityHour;

-- create new column name 'WeekDay'
EXEC sp_help daily_activity;

alter table daily_activity add WeekDay varchar(20);
go
update daily_activity set WeekDay = DATENAME(weekday, ActivityDate);
go

alter table merge_df add WeekDay varchar(20);
go
update merge_df set WeekDay = DATENAME(weekday, ActivityHour);
go

alter table sleep_day add WeekDay varchar(20);
go
update sleep_day set WeekDay = DATENAME(weekday, SleepDay);
go

-- Need to extract the ActivityHour column by creating a separate DateHour column for it.

alter table merge_df add DateHour int

select Id, ActivityHour, StepTotal, Calories, WeekDay, DATEPART(hour, ActivityHour) as DateHour
from merge_df
update merge_df set DateHour = DATEPART(hour, ActivityHour);
go

select *, CAST(ActivityHour as date) as ActivityDay
from merge_df

alter table merge_df add ActivityDay date

update merge_df set ActivityDay = CAST(ActivityHour as date);
go

-- Now drop ActivityHour column from merge_df.

alter table merge_df
drop column ActivityHour


-- We have to merge sleep_day and merge_df together.
-- First Rename SleepDay column to ActiviyDay for match merge_df ActivityDay column

EXEC sp_rename 'sleep_day.SleepDay', 'ActivityDay', 'COLUMN';

-- now let's merge sleep_day, merge_df and create new table merge_df2. 

select m.Id, m.ActivityDay, m.StepTotal, m.Calories, 
		m.WeekDay, m.DateHour, s.TotalSleepRecords, 
		s.TotalMinutesAsleep, s.TotalTimeInBed
into merge_df2
from merge_df as m
inner join sleep_day as s
on m.Id = s.Id and m.ActivityDay = s.ActivityDay

--Let's also make some changes to the daily_activity dataframe. We'll start by creating 3 additional columns (TotalActiveMinutes, TotalMinutes, and TotalActiveHours). Then we'll remove some columns we won't need.

alter table daily_activity add TotalActiveMinutes int
alter table daily_activity add TotalMinutes int
alter table daily_activity add TotalActiveHours float


SELECT Id, ActivityDate, WeekDay, TotalSteps, TotalDistance,
		VeryActiveDistance, ModeratelyActiveDistance, LightActiveDistance,
		SedentaryActiveDistance, VeryActiveMinutes, FairlyActiveMinutes,
		LightlyActiveMinutes, SedentaryMinutes, 
		SUM(VeryActiveMinutes+FairlyActiveMinutes+LightlyActiveMinutes) as TotalActiveMinutes,
		SUM(VeryActiveMinutes+FairlyActiveMinutes+LightlyActiveMinutes+SedentaryMinutes) as TotalMinutes,
		cast(ROUND((SUM(VeryActiveMinutes+FairlyActiveMinutes+LightlyActiveMinutes))/60.0, 2) as int) as TotalActiveHours, Calories
into daily_activity2
FROM daily_activity
GROUP BY Id, ActivityDate, WeekDay, TotalSteps, TotalDistance,
		VeryActiveDistance, ModeratelyActiveDistance, LightActiveDistance,
		SedentaryActiveDistance, VeryActiveMinutes, FairlyActiveMinutes,
		LightlyActiveMinutes, SedentaryMinutes, Calories;

-- let't do some statistical info about them.

select AVG(TotalSteps) as AVGTotalSteps,
		AVG(TotalActiveMinutes) as AVGTotalActiveMinutes,
		AVG(TotalMinutes) as AVGTotalMinutes,
		AVG(TotalActiveHours) as AVGTotalActiveHours,
		AVG(Calories) as AVGCalories,
		MAX(TotalSteps) as MAXTotalSteps,
		MAX(TotalActiveMinutes) as MAXTotalActiveMinutes,
		MAX(TotalMinutes) as MAXTotalMinutes,
		MAX(TotalActiveHours) as MAXTotalActiveHours,
		MAX(Calories) as MAXCalories,
		MIN(TotalSteps) as MINTotalSteps,
		MIN(TotalActiveMinutes) as MINTotalActiveMinutes,
		MIN(TotalMinutes) as MINTotalMinutes,
		MIN(TotalActiveHours) as MINTotalActiveHours,
		MIN(Calories) as MINCalories
from daily_activity2

select AVG(StepTotal) as AVGstepTotal,
		AVG(Calories) as AVGCalories,
		AVG(DateHour) as AVGDateHour,
		MAX(StepTotal) as MAXStepTotal,
		MAX(Calories) as MAXCalories,
		MAX(DateHour) as MAXTotalMinutes,
		MIN(StepTotal) as MINDateHour,
		MIN(Calories) as MINCalories,
		MIN(DateHour) as MINDateHour
from merge_df




select * from daily_activity2;
select * from merge_df;
select * from merge_df2;




begin tran
rollback
select @@TRANCOUNT
commit



