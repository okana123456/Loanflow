import assert from 'node:assert/strict';
import fs from 'node:fs';
import { PGlite } from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';

const db=new PGlite();
const authId='11111111-1111-4111-8111-111111111111';
await db.exec(`
create schema auth;
create function auth.uid() returns uuid language sql stable as $$ select '${authId}'::uuid $$;
create function auth.jwt() returns jsonb language sql stable as $$ select '{"email":"william@example.test"}'::jsonb $$;
create table public.loan_staff(id uuid primary key,business_id text,email text,auth_user_id uuid,is_active boolean);
create table public.bripta_staff_permissions(staff_id uuid,view_accounting boolean);
create function public.bripta_has_role(text) returns boolean language sql stable as $$ select false $$;
insert into public.loan_staff values ('22222222-2222-4222-8222-222222222222','BIZ-B3F5E5D9',
  'william@example.test','${authId}',true);
insert into public.bripta_staff_permissions values ('22222222-2222-4222-8222-222222222222',true);
`);
await db.exec(fs.readFileSync(new URL('../bripta-inactive-staff-permissions-20261114.sql',import.meta.url),'utf8'));
assert.equal((await db.query("select public.bripta_has_permission('view_accounting') as allowed")).rows[0].allowed,true);
await db.exec("update public.loan_staff set is_active=false where email='william@example.test'");
assert.equal((await db.query("select public.bripta_has_permission('view_accounting') as allowed")).rows[0].allowed,false);
await db.exec(fs.readFileSync(new URL('../bripta-inactive-staff-permissions-20261114.sql',import.meta.url),'utf8'));
assert.equal((await db.query("select public.bripta_has_permission('view_accounting') as allowed")).rows[0].allowed,false);
await db.close();
console.log('PASS: inactive Bripta staff lose explicit database permission; rerun is safe.');
