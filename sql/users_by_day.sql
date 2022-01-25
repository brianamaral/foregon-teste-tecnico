SELECT count(DISTINCT _id)
	,date_trunc('day', "createdat_$date")
FROM user_data_products
GROUP BY date_trunc('day', "createdat_$date")
ORDER BY date_trunc('day', "createdat_$date");