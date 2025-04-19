-- SQL Project - Data Cleaning

-- Create a table layoffs_working
create table layoffs_working
like layoffs;

select * from layoffs_working;

-- Insert data layoffs_working from layoffs
insert layoffs_working
select * from layoffs;

-- ----------------------------------------------------------------------------------

-- Remove Duplicates

-- This query is check for duplicates
select company,location,industry,total_laid_off,percentage_laid_off,date,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date' ) as row_num
from layoffs_working;

select * 
from(
	select company,location,industry,total_laid_off,percentage_laid_off,date,
	row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date' ) as row_num
	from layoffs_working
) as duplicates
where row_num > 1;

-- let's just look at oda to confirm
select * from layoffs_working
where company = 'Oda';

-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
select * 
from(
	select company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions,
	row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions ) as row_num
	from layoffs_working
) as duplicates
where row_num > 1;

select * from layoffs_working
where company = 'Casper';

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- now you may want to write it like this:
with delete_cte as
(
	select * 
	from(
		select company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions,
		row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions ) as row_num
		from layoffs_working
	) as duplicates
	where row_num > 1
)
delete from delete_cte;

with delete_cte as
(
	select company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions,
	row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions ) as row_num
)
delete from layoffs_working
where (company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions,row_num) in 
(select company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions,row_num
from delete_cte
)and row_num > 1;

-- Add column row_num and datatype int
alter table layoffs_working
add row_num int;

select * from layoffs_working;

-- Create a new table layoffs_working2
CREATE TABLE `layoffs_working2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` bigint DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_working2;

-- Insert data into layoffs_working2
INSERT INTO `my_data`.`layoffs_working2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
select company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions,
		row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions ) as row_num
from layoffs_working;

-- duplicates data in the table
select * from layoffs_working2
where row_num >= 2;

-- now that we have this we can delete rows were row_num is greater than 2
delete from layoffs_working2
where row_num >=2;

-- -------------------------------------------------------------------------------

-- Standardize data

select * from layoffs_working2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
select distinct industry 
from layoffs_working2
order by industry;

select industry 
from layoffs_working2
where industry='CryptoCurrency';

-- Industry is null & blank
select *
from layoffs_working2
where industry is null
or industry = ''
order by industry;

-- a look at these
select *
from layoffs_working2
where company like 'Bally%';

select *
from layoffs_working2
where company like 'Airbnb%';

-- set the blanks to nulls since those are typically easier to work with
update layoffs_working2
set industry = null
where industry = '';

-- now check those are all null
select *
from layoffs_working2
where industry is null
or industry = ''
order by industry;

-- now need to populate those nulls if possible using join 
update layoffs_working2 t1
join layoffs_working2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- and check it looks like Bally's was the only one without a populated row to populate this null values

select *
from layoffs_working2
where industry is null
or industry = ''
order by industry;

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
select distinct industry
from layoffs_working2
order by industry;

update layoffs_working2
set industry = 'Crypto'
where industry in ('Crypto Currency','CryptoCurrency');

-- now that's taken care of:
select distinct industry
from layoffs_working2
order by industry;

-- we also need to look at 
select * from layoffs_working2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
select distinct country
from layoffs_working2
order by country;

-- Update table using trim function
update layoffs_working2
set country = trim(trailing '.' from country);

-- Query the country order by country
select distinct country
from layoffs_working2
order by country;

select date from layoffs_working2;

-- update table by using str_to_date function
update layoffs_working2
set date = str_to_date(date, '%m/%d/%Y');

-- modify column datatype text to date
alter table layoffs_working2
modify column date date;

select * from layoffs_working2;

-- -------------------------------------------------------------

-- remove any columns and rows we need to

-- Check null valuse
select * from layoffs_working2
where total_laid_off is null
and percentage_laid_off is null;

-- Delete Useless data we can't really use
delete from layoffs_working2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_working2;

-- Drop column row_num
alter table layoffs_working2
drop column row_num;

-- Final clean dataset
select * from layoffs_working2;






