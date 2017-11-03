drop table if exists users;
create table users (
  id integer primary key autoincrement,
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