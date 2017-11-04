drop table if exists users;
create table users (
  user_id integer primary key autoincrement,
  username text not null,
  password text not null,
  first_name text not null,
  last_name text not null,
  institution text not null,
  team text not null,
  email text not null,
  staff_id text not null
);
insert into users (
  username, 
  password, 
  first_name, 
  last_name, 
  institution, 
  team, 
  email, 
  staff_id) 
values (
  'cindytay',
  '123456',
  'Cindy',
  'Tay',
  'National University of Singapore',
  'REA Team',
  'dprcindy@nus.edu.sg',
  '123xxxx');
drop table if exists activities;
create table activities (
  activity_id integer primary key autoincrement,
  activity_timestamp text not null,
  user_id integer not null,
  activity_name text not null,
  remark text not null
);
drop table if exists benchmarks;
create table benchmarks (
  benchmark_id integer primary key autoincrement,
  benchmark_timestamp text not null,
  user_id integer not null,
  name text not null,
  department text not null,
  position text not null,
  metrics text not null
);

CREATE TABLE IF NOT EXISTS process (
  crawler_name TEXT PRIMARY KEY,
  processing INT NOT NULL
);

CREATE TABLE IF NOT EXISTS profiles (
  profile_link TEXT PRIMARY KEY,
  name TEXT,
  department TEXT,
  university TEXT,
  tag TEXT,
  position TEXT,
  phd_year TEXT,
  phd_school TEXT,
  promotion_year TEXT,
  text_raw TEXT,
  user_updated INTEGER
);
