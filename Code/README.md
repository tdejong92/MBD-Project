# Readme

Readme of the final project for Managing Big Data of Tanja de Jong, s1091131, and Marlène Hol, s1317210.

## The delivered files and directory

In the root directory there is besides the README.md one other file included: pom.xml. This pom.xml contains almost all dependencies needed to run the software. When using maven package, this pom.xml need to be used (see next section). The other dependency necessary is the JSONParser, which is included in the directory src>main>resources. All the code developed for this project can be found in the directory src>main>java>marlene>bigdata. There are four java classes included in the project. First, there is the class Fair which is created to represent a specific fair and the class PrintableArrayWritable, which is used to use the values for the number of mentions and sentiment in one program. The other two classes are MapReducePrograms.   AllTweets.java contains a MapReduce program to determine the number of tweets in a dataset. Top5FairsComplete.java contains the MapReduce program to determine the number of mentions of the top 5 fairs and the sentiment of those mentions. 

The program can be imported to the hadoop cluster by inserting the entire directory structure in a self chosen folder. 


## Compiling the program

The program can be compiled by using mvn package in the root directory. Maven need to be installed for this purpose, but in order to reach the data set the program needs to be imported on the Hadoop Cluster of the University of Twente. On this cluster maven is installed. After using maven package in the root directory the folder target appears. In this folder bigdata-1.0.jar appears.

## Executing compiled programs

The program need to be executed on the hadoop cluster of the University of Twente. We executed the code per year, but it is also possible to perform the operation on the entire dataset. Tbe program Top5FairsComplete can be executed by using the following command when you are in the root directory:

hadoop jar target/bigdata-1.0.jar marlene.bigdata.Top5FairsComplete /data/twitterNL/2012* output2012

In this case the program is executed for 2012, but if you want to run it for another year you can change the year after /data/twitterNL/. The last word, output2012, is the name given to the directory for the output. This can be altered if another name is preferred. THe output is put on the hadoop cluster.

The program allTweets can be executed by using the following command:

hadoop jar target/bigdata-1.0.jar marlene.bigdata.AllTweets /data/twitterNL/2012* outputTweets2012

Also this program is executed for 2012 and has as chosen name outputTweets2012. This can be altered as described above.