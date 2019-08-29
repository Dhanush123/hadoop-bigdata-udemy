ratings = LOAD '/user/maria_dev/ml-100k/u.data' 
AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRealese:chararray, imdblink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, 
ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;
ratingsStats = FOREACH ratingsByMovie GENERATE group as movieID, COUNT(ratings.rating) as numRating, AVG(ratings.rating) as avgRating;

results = FILTER ratingsStats BY avgRating < 2.0;
resultsJoined = JOIN results BY movieID, nameLookup BY movieID;
resultsSorted = ORDER resultsJoined BY numRating DESC;

DUMP resultsSorted;
