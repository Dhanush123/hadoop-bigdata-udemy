from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
            # ,
            # MRStep(reducer=self.reducer_sorted_output)
        ]

    def mapper_get_ratings(self, _, line):
        userID, movieID, rating, timestamp = line.split('\t')
        yield rating, 1

    def reducer_count_ratings(self, movieID, num_ratings):
        yield str(sum(num_ratings)).zfill(5), movieID

    def reducer_sorted_output(self, count, movieIDs):
        for movieID in movieIDs:
            yield movieID, count


if __name__ == '__main__':
    RatingsBreakdown.run()

# Ran on Hadoop "cluster" with following command:
# python RatingsBreakdown.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar u.data
