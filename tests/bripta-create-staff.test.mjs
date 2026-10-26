import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';
import vm from 'node:vm';

const source = stripTypeScriptTypes(readFileSync(new URL('../supabase/functions/bripta-create-staff/index.ts', import.meta.url), 'utf8')
  .replace(/^import .*;\r?\n/gm, ''));

async function run({ actorRole = 'admin', staffError = null, existing = false } = {}) {
  const calls = [];
  let handler;
  const client = {
    auth: {
      getUser: async () => ({ data: { user: { id: 'actor-auth', email: 'admin@bripta.test' } }, error: null }),
      admin: {
        createUser: async () => { calls.push('auth-create'); return { data: { user: { id: 'new-auth' } }, error: null }; },
        deleteUser: async () => { calls.push('auth-delete'); return { error: null }; },
      },
    },
    from(table) {
      const q = {
        eq() { return q; }, ilike() { return q; }, limit() { return q; }, select() { return q; },
        maybeSingle: async () => ({ data: table === 'loan_staff' ? { id: 'actor', role: actorRole, is_active: true, business_id: 'BIZ-B3F5E5D9' } : { id: 'branch' }, error: null }),
        then(resolve) { return Promise.resolve({ data: existing ? [{ id: 'existing' }] : [], error: null }).then(resolve); },
        insert(value) { calls.push([table, 'insert', value]); return q; },
        upsert(value) { calls.push([table, 'upsert', value]); return { error: null }; },
        delete() { calls.push([table, 'delete']); return q; },
        single: async () => ({ data: staffError ? null : { id: 'new-staff' }, error: staffError }),
      };
      return q;
    },
  };
  const context = {
    serve: fn => { handler = fn; }, createClient: () => client, Response, Request, Set, Date,
    Deno: { env: { get: name => name === 'SUPABASE_URL' ? 'https://example.test' : 'key' } },
    console: { error() {} },
  };
  vm.runInNewContext(source, context);
  const response = await handler(new Request('https://example.test/create', {
    method: 'POST', headers: { authorization: 'Bearer valid-token' },
    body: JSON.stringify({ name: 'New Officer', email: 'new@bripta.test', password: 'long-password',
      branch_id: 'branch', role: 'loan_officer', is_active: true, permissions: {} }),
  }));
  return { status: response.status, data: await response.json(), calls };
}

let result = await run();
assert.equal(result.status, 200);
assert.equal(result.data.staff_id, 'new-staff');
assert.equal(result.calls.filter(c => c === 'auth-create').length, 1);
assert.equal(result.calls.filter(c => c === 'auth-delete').length, 0);
assert.equal(result.calls.find(c => Array.isArray(c) && c[0] === 'loan_staff')[2].auth_user_id, 'new-auth');

result = await run({ actorRole: 'loan_officer' });
assert.equal(result.status, 403);
assert.equal(result.calls.length, 0);

result = await run({ staffError: { message: 'RLS denied' } });
assert.equal(result.status, 500);
assert.equal(result.calls.filter(c => c === 'auth-delete').length, 1);

result = await run({ existing: true });
assert.equal(result.status, 409);
assert.equal(result.calls.length, 0);

console.log('PASS: Bripta staff creation, admin access, duplicate prevention and login rollback');
