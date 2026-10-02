import {PGlite} from '../.recovery-test-runtime/node_modules/@electric-sql/pglite/dist/index.js';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const sql=readFileSync(new URL('../bripta-rename-head-office-migori-20261002.sql',import.meta.url),'utf8');
const db=new PGlite();
try{
  await db.exec(`create table bripta_branches(id uuid primary key,business_id text,name text,is_head_office boolean,updated_at timestamptz);
    insert into bripta_branches values
    ('00000000-0000-4000-8000-000000000001','BIZ-B3F5E5D9','Head Office',true,null),
    ('00000000-0000-4000-8000-000000000002','OTHER','Head Office',true,null);`);
  await db.exec(sql);
  const first=(await db.query('select * from bripta_branches order by id')).rows;
  assert.equal(first[0].name,'Migori');assert.equal(first[1].name,'Head Office');
  await db.exec(sql);
  assert.deepEqual((await db.query('select * from bripta_branches order by id')).rows,first);
  await db.exec("update bripta_branches set business_id='OTHER' where name='Migori'");
  await assert.rejects(db.exec(sql),/Expected Bripta Head Office/);
  await db.exec('rollback');
  console.log('PASS: Migori rename is idempotent, preserves branch ID, and refuses another business.');
}finally{await db.close();}
