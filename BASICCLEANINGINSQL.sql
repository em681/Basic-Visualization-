SELECT * 
FROM layoffs22.layoffs;

CREATE TABLE layoffs22.layoffs_stagings 
LIKE layoffs22.layoffs;

INSERT layoffs_stagings
SELECT * FROM layoffs22.layoffs;

SELECT *
FROM layoffs22.layoffs_stagings
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		layoffs22.layoffs_stagings;



SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		layoffs22.layoffs_stagings
) duplicates
WHERE 
	row_num > 1;
    
SELECT *
FROM layoffs22.layoffs_stagings
WHERE company = 'Oda'
;

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs22.layoffs_stagings
) duplicates
WHERE 
	row_num > 1;
SELECT*
FROM layoffs_stagings
WHERE company="CASPER";

DELETE 
FROM duplicate_cte
WHERE row_num >1;

CREATE TABLE `layoffs22`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `layoffs22`.`layoffs_staging2`
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
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs22.layoffs_stagings;
DELETE FROM layoffs22.layoffs_staging2 
WHERE row_num >= 2;

SELECT*
FROM layoffs22.layoffs_staging2;

SELECT *
FROM layoffs22.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs22.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs22.layoffs_staging2
WHERE company LIKE 'airbnb%';

UPDATE layoffs22.layoffs_staging2
SET industry = NULL
WHERE industry = '';


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs22.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT DISTINCT industry
FROM layoffs22.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT DISTINCT industry
FROM layoffs22.layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs22.layoffs_staging2;

SELECT DISTINCT country
FROM layoffs22.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT DISTINCT country
FROM layoffs22.layoffs_staging2
ORDER BY country;

SELECT *
FROM world_layoffs.layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs22.layoffs_staging2;

SELECT *
FROM layoffs22.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM layoffs22.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE FROM layoffs22.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs22.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM layoffs22.layoffs_staging2;
