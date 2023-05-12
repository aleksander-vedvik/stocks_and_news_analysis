publish:
	rsync -rlptoDvogP \
	--delete \
	--filter='P data' \
	--chown=ubuntu:ubuntu \
	--exclude '.git*' \
	--exclude 'data/' \
	--exclude 'jupyter/' \
	--exclude 'venv/' \
	. namenode:project

news: clean-news-output
	python3 ./src/hadoop/news_article_mp.py \
	--hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.5.jar \
	-r hadoop hdfs:///data/news/*.json \
	--output-dir hdfs:///out/news \
	--no-output

	hadoop fs -mv /out/news/part-00000 /out/news/news.csv

main: clean-stock-delta
	spark-submit ~/project/src/spark/main.py

sentiment:
	spark-submit ~/project/src/spark/sentiment_analysis.py

show:
	spark-submit ~/project/src/spark/show.py

clean-news-output:
	-hadoop fs -rm -r /out/news

copy-to-hadoop: clean-data
	hadoop fs -mkdir /data
	hadoop fs -put ./data/* /data

clean-stock-delta:
	-hadoop fs -rm -r /data/delta
	-hadoop fs -mkdir /data/delta

clean-hdfs: clean-all-output clean-data

clean-all-output: clean-word-count-output clean-news-output

clean-data:
	-hadoop fs -rm -r /data

stocks: clean-stock-output
	python3 ./src/hadoop/stocks_mr_job.py \
	--hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.5.jar \
	-r hadoop hdfs:///data/stocks/*/json/* \
	--output-dir hdfs:///out/stocks \
	--no-output

	hadoop fs -mv /out/stocks/part-00000 /out/stocks/stocks.csv

clean-stock-output:
	-hadoop fs -rm -r /out/stocks
	
requirements:
	python3 -m pip install -r ~/requirements.txt 

cleanup:
	-hadoop fs -rm -r /out/news/news.parquet

pipeline: 
	make stocks
	make news
	make sentiment
	make main
	make show	
	make cleanup
