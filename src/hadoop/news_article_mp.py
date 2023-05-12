from models.news_article import NewsArticle
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol


class MRNewsarticle(MRJob):
    DIRS = ['models']
    OUTPUT_PROTOCOL = RawProtocol

    def mapper_init(self):
        self.in_article = False
        self.content = NewsArticle()

    def mapper(self, _, line):
        key, value = NewsArticle.clean_and_split(line)
        if not self.in_article and key == "{":
            self.in_article = True
        if self.in_article and (key == "}" or key == "},"):
            yield None, self.content.to_csv()
            self.in_article = False
            self.content = NewsArticle()
        self.content.match(key, value)

    def reducer(self, key, values):
        # values will always be exactly 1 element:
        # obj = next(values)
        # yield key, [value for value in obj]
        for val in values:
            yield None, val


if __name__ == "__main__":
    MRNewsarticle.run()
