** Project created following CodewithYu **

  1. Install Required Packages :

     pip install confluent-kafka==2.3.0
     pip install pyspark==3.5.0
     pip install simplejson==3.19.2
     pip install py4j==0.10.9.7

2. Run docker in detached mode
   docker-compose up -d
   
3. Make sure pyspark and docker image versions match
4. run the below command to push manufactured data to Kafka topics

   python jobs/main.py
   
6. Run the below command to submit the spark job

    docker exec -it smartcity-spark-master-1 spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk:1.11.469 .\jobs\city-streaming.py
   

