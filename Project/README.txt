AUTHOR: Kartik Mahaley, Shakti Patro, Pankaj Tripathi, Chen Bai
VERSION: 1.0

DESCRIPTION: 
Our program consist of mapreduce framework which can be used to write your own mapreduce code. Phases of our code is 
1. Creating and running Amazon Web Services EC2 instances
2. Read data and develope mapper
3. Shuffle and sort
4. Develope reducer

CONTENTS:
	1. Source code
	2. Config.properties and log4j.properties
	3. README.txt
	4. Project Report.pdf
	5. Makefile
	6. Examples Folder with source code, input and output files
	7. Project Report.rmd

SYSTEM SPECIFICATION & PRE REQUISITES:
	1. Mac/Linux machine with 8GB RAM
	2. Java 1.7
	3. Apache Maven 3.3
	4. Any IDE example eclipse	

HOW TO USE API:
1) Add jar to classpath or add it to pom.xml
	For adding it to pom, you can use following command 
	make addmaven
2) Implement Mapper and Reducer class. 
3) Run app's run method
 
BEFORE YOU RUN: 
Our program requires Amazon Web Services credentials(secret key, access key, region name, pem keys, bucket name etc.) Before using our framework please create AWS account with all these details. Please fill all the details in config.properties file which is read by our mapreduce framework. 

	1) Please don't change ipfile name in this config.properties file.
	2) Make sure output folder exist in s3 bucket and is empty
	3) Fill config.properties file a mentioned below:
	4) Make sure the EC2 instances are available in specified region for which you have pem keys

		key=<SECRET KEY>
		password=<ACCESS KEY>
		bucket=<BUCKET NAME>
		inputfile=<INPUT FILE FOLDER>
		ipfile=ipaddress.txt
		output=<OUTPUT FOLDER NAME>
		action=<START/STOP>
		instancenumber=<NUMBER OF INSTANCES>
		pem=<PEM KEYS>
		instancetype=<TYPE OF INSTANCE>
		pemKeysPath=<PEM KEY PATH>
		jarPath=<JAR NAME>

		For example,
		key=A***************
		password=A**************************
		bucket=mybucketname
		inputfile=myfiles/input/
		ipfile=ipaddress.txt
		output=output
		action=start
		instancenumber=9
		pem=pemkeyname
		instancetype=t2.micro
		pemKeysPath=/home/user/Documents/
		jarPath=node.jar

STEPS TO EXECUTE:
1) Through terminal goto folder where jar, log4j.properties, config.properties are present
2) Specify action=start, instancetype and instancenumber in config.properties and execute jar as
	java -jar jarname.jar
3) To stop instances, specify action=stop in config.properties and execute jar as 
	java -jar jarname.jar


--If you want to create your own jar you can use command 
	make buildjar

