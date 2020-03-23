-- sql script to initialize the message_queue table
-- run statements from this script as needed

-- create a schema to hold the msg_queue table
CREATE SCHEMA `s71` DEFAULT CHARACTER SET utf8mb4 ;
drop table if exists s71.msg_queue;

-- create the msg_queue table
create table s71.msg_queue (
    msg_id int unsigned not null primary key auto_increment
       comment 'unique id - lookup individual messages with this id',
    msg_type varchar(100) not null
       comment 'short type name for message. messages of the same type have compatible msg_data',
    msg_status enum('PUSHED', 'POPPED', 'CONFIRMED') not null default 'PUSHED'
       comment 'PUSHED - this message is ready to be popped from the queue
                POPPED - this message has been popped, but not yet confirmed
                CONFIRMED - this message has been confirmed as popped,
                *Note: status must be considered in conjuction with ready_after and error_message',
    ready_after timestamp not null default current_timestamp
       comment 'messages in the "READY"|"RUNNING" status can be "popped" when ready_after >= current_timestamp.
                This column is used to implement future scheduled messages and message TTL',
    msg_data json
       comment 'Optional metadata used to execute the message',

    pushed_on timestamp not null default current_timestamp
       comment 'TS message was "pushed" unto the queue. For debug/visibility',
    pushed_by varchar(100) not null
       comment 'Client name that "pushed" the message',

    popped_on timestamp
       comment 'TS message was "peeked" from the queue. For debug/visibility',
    popped_by varchar(100)
       comment 'Client name that "popped" the message',

    confirmed_on timestamp
       comment 'TS message was "confirmed" from the queue. For debug/visibility',

    error_message text
       comment 'Stack trace or error message when message is in "ERROR" status'
);

create index msg_ready
on s71.msg_queue (
  msg_status, ready_after, msg_type
)
comment 'Index used to peek top N ready messages';


-- grant access to table to user who will be connecting
-- please change the user to whatever you setup here
-- grant all privileges on s71.msg_queue to 's71'@'%';
-- flush privileges;
