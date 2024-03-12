import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import { $toData, Table, queryArgs, sql } from './index.js'
import { uuidv7 } from '@daaku/uuidv7'

const expectIndexOp = (db: Database, sql: string, ...args: any[]) => {
  const explain = db.query(sql)
  expect(explain.all(...args).map((v: any) => v.opcode)).toContain('IdxGT')
}

const withoutID = <T extends Object & { id?: string }>(d: T): T => {
  delete d.id
  return d
}

test.each([
  '$id',
  '$id = 42',
  '$phone.home = 42',
  '$phone_home = 42',
  '$name = "foo" and $age = 42',
  '$.foo',
  '$a',
  '$.',
])('$toData: %s', (v: string) => {
  expect($toData(v)).toMatchSnapshot()
})

test.each([
  ['where name', sql`where $name = ${'yoda'}`],
  ['where name and age', sql`where $name = ${'yoda'} and $age = ${42}`],
  ['limit', sql`limit ${42}`],
])('sql: %s', (_, sqlParts) => {
  expect(sqlParts).toMatchSnapshot()
})

interface Jedi {
  id?: string
  name: string
  age: number
  gender?: 'male' | 'female'
}
const YODA: Jedi = Object.freeze({
  id: uuidv7(),
  name: 'yoda',
  age: 900,
  gender: 'male',
})

const makeJedis = (): Table<Jedi> => {
  const jedis = new Table<Jedi>(new Database(':memory:'), 'jedi')

  jedis.insert(YODA)
  jedis.insert({ name: 'luke', age: 42, gender: 'male' })
  jedis.insert({ name: 'leia', age: 42, gender: 'female' })
  jedis.insert({ name: 'grogu', age: 50, gender: 'male' })
  jedis.insert({ name: 'rey', age: 32, gender: 'female' })

  return jedis
}

test('insert returns object with id', () => {
  const jedis = makeJedis()
  const yoda = { name: 'yoda', age: 900 }
  const inserted = jedis.insert(yoda)
  expect(inserted.id).toBeDefined()
  expect(inserted).toMatchObject(yoda)
})

test('index is used', () => {
  const jedis = makeJedis()
  expectIndexOp(
    jedis.db,
    "explain select data from jedi where data->>'id' = ?",
    YODA.id,
  )
})

test('getByID', () => {
  const jedis = makeJedis()
  const fetched = jedis.getByID(YODA.id!)
  expect(fetched).toEqual(YODA)
})

test('get', () => {
  const jedis = makeJedis()
  expect(jedis.get(sql`where $id = ${YODA.id}`)?.name).toBe(YODA.name)
  expect(jedis.get(sql`where $age = 50`)?.name).toBe('grogu')
})

test('get helper', () => {
  const jedis = makeJedis()
  const byAgeAndGender = (
    jedis: Table<Jedi>,
    age: number,
    gender: 'male' | 'female',
  ): Jedi | undefined =>
    jedis.get(sql`where $age = ${age} and $gender = ${gender}`)
  expect(byAgeAndGender(jedis, 42, 'male')?.name).toBe('luke')
  expect(byAgeAndGender(jedis, 42, 'female')?.name).toBe('leia')
})

test('all', () => {
  const jedis = makeJedis()
  expect(jedis.all(sql`where $age > ${42}`).map(withoutID)).toMatchSnapshot()
})

test('delete', () => {
  const jedis = makeJedis()
  expect(jedis.all(sql`where $age = 42`).map(withoutID)).toMatchSnapshot()
  jedis.delete(sql`where $age = 42`)
  expect(jedis.all(sql`where $age = 42`).map(withoutID)).toMatchSnapshot()
})

test('patch', () => {
  const jedis = makeJedis()
  expect(jedis.all(sql`where $age = 42`).map(withoutID)).toMatchSnapshot()
  jedis.patch({ name: 'dead' }, sql`where $age = 42`)
  expect(jedis.all(sql`where $age = 42`).map(withoutID)).toMatchSnapshot()
})

test('replace', () => {
  const jedis = makeJedis()
  expect(jedis.all(sql`where $age = 42`).map(withoutID)).toMatchSnapshot()
  jedis.replace({ name: 'dead', age: 42 }, sql`where $age = 42`)
  expect(jedis.all(sql`where $age = 42`).map(withoutID)).toMatchSnapshot()
})

test('custom update age + 1', () => {
  const jedis = makeJedis()
  expect(jedis.all(sql`where $age = 42`).map(withoutID)).toMatchSnapshot()
  const [query, args] = queryArgs(
    'update',
    jedis.table,
    sql`set data = jsonb_replace(data, '$.age', $age + 1)`,
  )
  jedis.db.query(query).run(...args)
  expect(jedis.all(sql`where $age = 43`).map(withoutID)).toMatchSnapshot()
})
