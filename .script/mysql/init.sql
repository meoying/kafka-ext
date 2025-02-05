create database if not exists kafka_ext;

create table kafka_ext.delay_msgs
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
    on kafka_ext.delay_msgs (send_time, status);

-- 用于测试分库分表
CREATE DATABASE IF NOT EXISTS kafka_ext_db_0;
CREATE TABLE IF NOT EXISTS kafka_ext_db_0.delay_msgs_tab_0 LIKE kafka_ext.delay_msgs;
CREATE TABLE IF NOT EXISTS kafka_ext_db_0.delay_msgs_tab_1 LIKE kafka_ext.delay_msgs;

CREATE DATABASE IF NOT EXISTS kafka_ext_db_1;
CREATE TABLE IF NOT EXISTS kafka_ext_db_1.delay_msgs_tab_0 LIKE kafka_ext.delay_msgs;
CREATE TABLE IF NOT EXISTS kafka_ext_db_1.delay_msgs_tab_1 LIKE kafka_ext.delay_msgs;