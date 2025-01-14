create database if not exists kafka_ext;

create table delay_msgs
(
    id         bigint auto_increment
        primary key,
    `key`      varchar(191)                 null,
    biz        longtext                     null,
    data       text                         null,
    send_time  bigint                       null,
    status     tinyint unsigned default '0' null,
    send_count bigint                       null,
    ctime      bigint                       null,
    utime      bigint                       null,
    constraint uni_delay_msgs_key
        unique (`key`)
);

create index idx_send_time_status
    on delay_msgs (send_time, status);
