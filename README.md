### Application developed using Spark(3.0.1) and Scala(2.12.13)

###Steps to Execute
####Create a JAR file using the following command.
```shell
$sbt clean compile package
```
####Scenario 1: Movies data and 3 new columns which contain the max, min and average rating for that movie from the rating's data.
First argument is the input movie file.

Second argument is the input ratings file.

Third argument is the output directory.

```shell 
$spark-submit movielens_2.12-0.1.jar "C:\\Users\\gdkmu\\Downloads\\ml-1m\\movies.dat" "C:\\Users\\gdkmu\\Downloads\\ml-1m\\ratings.dat" "C:\\Users\\gdkmu\\Downloads\\ml-1m\\output\\"
```