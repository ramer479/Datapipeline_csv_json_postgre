/*
Question#1 : Could you find what is the gender ratio in each game?
*/

SELECT 
    fus.source,
    SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END) AS male_count,
    SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END) AS female_count,
	count(1) AS total_count,
    CAST (
		CAST( SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END) AS float) / CAST(COUNT(1) AS float) 
		AS DECIMAL(18,4)) AS male_ratio,
    CAST( 
		CAST(
			SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END) AS float) / CAST(COUNT(*) AS float) 
		AS DECIMAL(18,4)) AS female_ratio
FROM ovecell.fact_user fus
JOIN ovecell.dim_user ds ON fus.user_id = ds.user_id
GROUP BY fus.source;


/*
Question#2 : Try to list the youngest and oldest players per country.
*/
SELECT 
    dl.nationality as country,
    MIN(ds.dob) AS oldest_player_dob,
    MAX(ds.dob) AS youngest_player_dob
FROM 
ovecell.fact_user fus
JOIN ovecell.dim_user ds ON fus.user_id = ds.user_id
JOIN ovecell.dim_location dl ON fus.location_id = dl.location_id
GROUP BY dl.nationality;