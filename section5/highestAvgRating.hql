CREATE VIEW topMovieIDs AS
SELECT movieID, avg(rating) as avgRating, count(movieID) as countReviews
FROM ratings
GROUP BY movieID
ORDER BY avgRating DESC;

SELECT n.title, avgRating
FROM topMovieIDs t JOIN names n
ON t.movieID = n.movieID
WHERE countReviews > 10;