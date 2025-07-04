# Stream data analysis

## *news_dataset_MDA2024*
- Implementing a streaming data processing system using Structured Streaming in PySpark framework.
- Process the input data entered into the system continuously and in real time (real-time) from a news source and extract useful information from it.
- Filter the data flow based on different tasks
```markdown
Time: 2024-12-29 12:00:00
Category: PARENTING, Count: 1
Category: CULTURE & ARTS, Count: 1
Category: U.S. NEWS, Count: 5
Category: COMEDY, Count: 1
Category: WORLD NEWS, Count: 2

------------------------------
Time: 2024-12-29 12:00:20
Category: SPORTS, Count: 1
Category: CULTURE & ARTS, Count: 1
Category: U.S. NEWS, Count: 1
Category: WORLD NEWS, Count: 6
Category: TECH, Count: 1

------------------------------
...
```

## *web_streaming_dataset*:

- Get acquainted with the important algorithms in the analysis of stream data and implement these algorithms(DGIM & FM) in PySpark framework
- Estimate the number of 1 bits (successful user requests) in each window using the DGIM algorithm
<p align="center">
    <img src="1.png" alt="Descriptive Alt Text" class="fit-width-image">
</p>

- Using *FM* algorithm, estimate the number of unique users who have accessed the website.
```markdown
Actual number of unique users: 1491
Estimated number of unique users: 1575.3846153846155
```

## Persian Twitter Dataset(*dataset*)
- Both task and implementation of this section is provided as *Task3*
- Using PySpark's Structured Streaming, process new tweets in real-time and identify and count the hashtags of each tweet.
```markdown
+--------------------+--------------------+-------------+
|              window|             hashtag|hashtag_count|
+--------------------+--------------------+-------------+
|{2023-12-01 06:14...|         پرستو_معینی|            1|
|{2023-11-22 11:18...|        دلیران_میدان|            1|
|{2023-12-01 06:14...|          زهرا_صفایی|            1|
|{2023-11-10 07:10...|         درمان_سرطان|            1|
|{2023-11-10 07:10...|       سرطان_پروستات|            1|
...
```
- Use the sentiment feature of each tweet to analyze sentiment and calculate and display the average sentiment for each hashtag in real-time.
```markdown
+--------------------+-------------+
|             hashtag|avg_sentiment|
+--------------------+-------------+
|          حسن_روحانی|          0.0|
|         قیام_سراسری|          0.0|
|                 یمن|          0.0|
|       آرمیتا_گراوند|          0.0|
|    KingRezaPahlavi‌|          0.0|
...
```
