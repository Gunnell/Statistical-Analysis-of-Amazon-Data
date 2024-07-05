# Statistical Analysis on Big Data using Hadoop Multi-Node Cluster and MapReduce

## Project Overview

This project aims to perform statistical analysis on big data(Amazon electronics review data) utilizing a Hadoop multi-node cluster and the MapReduce programming model. The primary goal is to process and analyze large text files to compute statistical metrics such as Median, Mode, Standard Deviation, Range, and Average.

## Implementation Environment

- **Java Development Kit:** OpenJDK version 1.8.0_292
- **Apache Hadoop:** Version 3.2.1
- **Development Environment:** Eclipse IDE
- **Java Libraries Management:** Maven
- **Dependencies:** Hadoop MapReduce and Hadoop HDFS libraries
- **JAR File Generation:** Maven Assembly Plugin

## Hadoop Environment

The Hadoop environment is set up on three virtual machines:
- **NameNode:** 1 machine
- **DataNodes:** 2 machines
- **Operating System:** macOS (Docker was used to run the project on macOS)

## Use Case

This project is designed for analyzing large text files. In the Mapper phase, each word in the input file is counted and written to an output file. Various statistical calculations are then performed in the Reducer phase using this output file. The project includes Reducer functions for calculating Median, Mode, Standard Deviation, Range, and Average.

## Technical Challenges

Due to the large data size that cannot fit in main memory, several challenges were addressed:
- Calculating standard deviation involved computing the standard deviation of individual blocks in the Mapper phase, then combining these values in the Reducer phase to obtain the final standard deviation for the entire file.

## Implementation Details

The Java classes are organized by their usage. All Mapper and Reducer classes are located in the `hadoop.MapReducer` class.

## Performance Analysis

| Process             | Elapsed Time (328Mb) | Elapsed Time (951 bytes) |
|---------------------|-----------------------|--------------------------|
| Average             | 20                    | 16                       |
| Mode                | 22                    | 15                       |
| Median              | 16                    | 15                       |
| Range               | 15                    | 13                       |
| Standard Deviation  | 24                    | 16                       |

## Data Source

The data used for analysis was electronic ratings from Amazon, representing a large dataset of product reviews.

## Setup and Execution

### Prerequisites
- Docker
- Docker Compose
- Hadoop environment set up using Docker

### Environment Setup
1. **Set Environment Variables**
    ```sh
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    export HADOOP_HOME=/home/ubuntu/hadoop
    export HADOOP_CONF=$HADOOP_HOME/conf
    export PATH=$PATH:$JAVA_HOME:$HADOOP_HOME/bin
    ```

2. **Copy SSH Key to EC2**
    ```sh
    scp -i abc.pem /home/ubuntu/.ssh/id_rsa.pub ubuntu@ec2-34-207-78-241.compute-1.amazonaws.com:/home/ubuntu/.ssh/id_rsa.pub
    ```

3. **SSH into NameNode**
    ```sh
    ssh -i ~/Desktop/abc.pem ubuntu@ec2-54-242-192-245.compute-1.amazonaws.com
    ```

### Starting Hadoop Daemons
1. **Format NameNode**
    ```sh
    hdfs namenode -format
    ```

2. **Start HDFS**
    ```sh
    $HADOOP_HOME/sbin/start-dfs.sh
    ```

3. **Start YARN**
    ```sh
    $HADOOP_HOME/sbin/start-yarn.sh
    ```

4. **Start MapReduce Job History Server**
    ```sh
    $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver
    ```

### Stopping Hadoop Daemons
1. **Stop All Hadoop Services**
    ```sh
    $HADOOP_HOME/sbin/stop-all.sh
    ```

### Running the Project

1. **Start Docker Containers**
    ```sh
    docker-compose up -d
    ```

2. **Copy Files to NameNode**
    ```sh
    docker cp /path/to/subset.txt namenode:/tmp/
    docker cp /path/to/bdata3.jar namenode:/tmp/
    ```

3. **Access the NameNode Container**
    ```sh
    docker exec -it namenode /bin/bash
    ```

4. **HDFS Operations**
    - List HDFS directories
        ```sh
        hdfs dfs -ls /
        ```
    - Create user directory in HDFS
        ```sh
        hdfs dfs -mkdir -p /user/root
        ```
    - Upload input file to HDFS
        ```sh
        hdfs dfs -put /tmp/subset.txt /user/root/input/
        ```

5. **Run MapReduce Job**
    ```sh
    cd /tmp
    hadoop jar bdata3.jar com.project.bigdata.hadoop.Average /user/root/input /user/root/outputAvg
    ```

6. **View Output**
    ```sh
    hdfs dfs -cat /user/root/outputAvg/part-r-00000
    ```

7. **Shutdown Docker Containers**
    ```sh
    docker-compose down
    ```

8. **Copy Output Back to Host**
    ```sh
    docker cp namenode:/tmp/subset.txt /path/to/destination/rapor
    ```

