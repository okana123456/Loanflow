import assert from 'node:assert/strict';
import fs from 'node:fs';
import { PGlite } from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';

const db=new PGlite();
const james='e407a976-5b44-46e1-817a-95da54103aca';
await db.exec(`
create role anon; create role authenticated;
create schema auth;
create function auth.uid() returns uuid language sql stable as $$ select '${james}'::uuid $$;
create table auth.users(id uuid primary key,email text,last_sign_in_at timestamptz);
create table public.loan_staff(id uuid primary key,auth_user_id uuid,business_id text,is_active boolean,last_login timestamptz,name text,email text,role text,branch_id uuid);
create table public.bripta_branches(id uuid primary key,name text);
insert into public.bripta_branches values ('00000000-0000-4000-8000-000000000001','Mbita');
insert into auth.users values ('${james}','opiyoj128@gmail.com','2026-10-26 11:00:00+00');
insert into public.loan_staff values
  ('00000000-0000-4000-8000-000000000002','${james}','BIZ-B3F5E5D9',true,null,'James Opiyo','opiyoj128@gmail.com','loan_officer','00000000-0000-4000-8000-000000000001'),
  ('00000000-0000-4000-8000-000000000003',null,'OTHER_BUSINESS',true,null,'Other','other@example.test','loan_officer',null);
`);
const sql=fs.readFileSync(new URL('../bripta-staff-last-login-repair-20261026.sql',import.meta.url),'utf8');
await db.exec(sql);
assert.equal((await db.query("select last_login from loan_staff where name='James Opiyo'")).rows[0].last_login.toISOString(),'2026-10-26T11:00:00.000Z');
assert.equal((await db.query("select last_login from loan_staff where business_id='OTHER_BUSINESS'")).rows[0].last_login,null);
await db.query('select public.bripta_record_staff_login()');
assert.ok((await db.query("select last_login from loan_staff where name='James Opiyo'")).rows[0].last_login);
await db.exec(sql);
await db.close();
console.log('PASS: James Auth sign-in is backfilled; current Bripta login records activity; other businesses untouched; rerun safe.');
