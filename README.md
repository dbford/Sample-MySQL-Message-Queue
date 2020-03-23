## Table of contents
* [Table of contents](#table-of-contents)
* Project
  * [Description](#project)
  * [Requirements](#requirements)
  * [Features](#feature-set)
  * [Performance](#performance)
* [Development Notes](#development-notes)
* [File Structure](#important-files)
* [Setup / Run](#setup--running)


## Project
Sample implementation of a FiFo, multi-producer, multi-consumer, MySql based message queue. Implementation
is generic and can be used for multiple use cases. 

## Requirements 
> The challenge objective is to create a FiFo multi-queue with messages stored in a MySQL database, with an implementation that is independent of test data or use case.
  
>  In addition your application should...
>  1. Fill in the logic for all of the stubbed functions modifying the namespace as needed to develop the queue application.
>  2. Submit a SQL file that includes the table structure and any other data needed for your application.
>  3. Add additional functionality as needed so that the namespace can be run easily (start up script, examples, etc).
>  4. While not required, any notes on the development or optimizations you built in to your application would be a plus.

## Feature Set
1. MySql backed, FiFo message queue
1. Thread-safe API, multi-producer, multi-consumer 
1. Optimized to minimize locking and maximize throughput.
1. Debug/visibility metadata embedded into database records 


## Performance
The queue will always work, but some types of API usage will be more performant than others.
Specifically, codepaths are optimized for large batch sizes. Meaning calls like `queue.pop(limit = 100)` will be 
considerably faster than `queue.pop(limit = 1)`. 

For best performance, try to minimize the number of `PUSHED` messages on the queue at any time. This is most
easily done by having continually running consumers popping messages. If you expect to have a large number of
pushed messages, ensure that consumers are using a large batch size.

## Development Notes
I wasn't, and still am not, proficient with MySQL. This has been my first time using it, and much of my time has been
trying to convert knowledge of other RDMS to MySQL. For that reason, there may be mistakes or missed opportunities in the 
SQL queries, the table structure, or the transaction/locking mechanisms. These would probably be remedied with additional
experience. 

Also, as I do not yet know Closure, the Java language was used.

## Important files
| File | Description |
| ---- | ----------- |
| run_once.sql | Setup script that created that `s71` schema and `msg_queue` table |
| Message.java | Java POJO that holds a single msg_queue record |
| MessageQueue.java | Java translation of the specified closure API |
| MySqlMessageQueue.java | Actual MessageQueue implementation |
| MySqlMessageQueueTest.java | Unit tests for the message queue. There are also some unit tests in this file for generating
test data and measuring throughput | 


## Setup / Running
This project relies on access to an external MySQL server, Java 8+, and Maven. This project also relies on a hard-coded 
schema name to be used in the MySQL server.

Steps:
1. Have access to an external MySQL server.
1. Create a user account that the application will use to access the server.
1. Run the table creation script (located at <todo>)
1. Grant the previously created user access to the table.
    ```mysql
    grant all privileges on s71.msg_queue to 's71'@'%';
    flush privileges;
    ```
1. Put the JDBC connection string, containing the db url, user, and pass, into the mysql.properties file <todo>
   ```properties
   connectionString=jdbc:mysql://localhost:3306?user=s71&password=s71_USER_%21%40%23%24\
   ```
1. Run the example
   ```
   mvn clean install
```
