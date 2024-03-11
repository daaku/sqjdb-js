import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import {
  $toData,
  all,
  get,
  getByID,
  insert,
  patch,
  qCreateIndex,
  qCreateTable,
  queryArgs,
  remove,
  replace,
  sql,
} from './index.js'
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
const JEDI = 'jedi'
const YODA: Jedi = Object.freeze({
  id: uuidv7(),
  name: 'yoda',
  age: 900,
  gender: 'male',
})

const makeDB = (): Database => {
  const db = new Database(':memory:')
  db.query(qCreateTable(JEDI)).run()
  db.query(
    qCreateIndex({ table: JEDI, unique: true, expr: $toData('$id') }),
  ).run()

  insert<Jedi>(db, JEDI, YODA)
  insert<Jedi>(db, JEDI, { name: 'luke', age: 42, gender: 'male' })
  insert<Jedi>(db, JEDI, { name: 'leia', age: 42, gender: 'female' })
  insert<Jedi>(db, JEDI, { name: 'grogu', age: 50, gender: 'male' })
  insert<Jedi>(db, JEDI, { name: 'rey', age: 32, gender: 'female' })

  return db
}

test('insert returns object with id', () => {
  const db = makeDB()
  const yoda: Jedi = { name: 'yoda', age: 900 }
  const inserted = insert(db, JEDI, yoda)
  expect(inserted.id).toBeDefined()
  expect(inserted).toMatchObject(yoda)
})

test('index is used', () => {
  const db = makeDB()
  expectIndexOp(
    db,
    "explain select data from jedi where data->>'id' = ?",
    YODA.id,
  )
})

test('getByID', () => {
  const db = makeDB()
  const fetched = getByID<Jedi>(db, JEDI, YODA.id!)
  expect(fetched).toEqual(YODA)
})

test('get', () => {
  const db = makeDB()
  expect(get<Jedi>(db, JEDI, sql`where $id = ${YODA.id}`)?.name).toBe(YODA.name)
  expect(get<Jedi>(db, JEDI, sql`where $age = 50`)?.name).toBe('grogu')
})

test('get helper', () => {
  const db = makeDB()
  const byAgeAndGender = (
    db: Database,
    age: number,
    gender: 'male' | 'female',
  ): Jedi | undefined =>
    get<Jedi>(db, JEDI, sql`where $age = ${age} and $gender = ${gender}`)
  expect(byAgeAndGender(db, 42, 'male')?.name).toBe('luke')
  expect(byAgeAndGender(db, 42, 'female')?.name).toBe('leia')
})

test('all', () => {
  const db = makeDB()
  expect(
    all<Jedi>(db, JEDI, sql`where $age > ${42}`).map(j => j.name),
  ).toMatchSnapshot()
})

test('remove', () => {
  const db = makeDB()
  expect(
    all<Jedi>(db, JEDI, sql`where $age = 42`).map(j => j.name),
  ).toMatchSnapshot()
  remove(db, JEDI, sql`where $age = 42`)
  expect(
    all<Jedi>(db, JEDI, sql`where $age = 42`).map(j => j.name),
  ).toMatchSnapshot()
})

test('patch', () => {
  const db = makeDB()
  expect(
    all<Jedi>(db, JEDI, sql`where $age = 42`).map(j => j.name),
  ).toMatchSnapshot()
  patch(db, JEDI, { name: 'dead' }, sql`where $age = 42`)
  expect(
    all<Jedi>(db, JEDI, sql`where $age = 42`).map(j => j.name),
  ).toMatchSnapshot()
})

test('replace', () => {
  const db = makeDB()
  expect(
    all<Jedi>(db, JEDI, sql`where $age = 42`).map(withoutID),
  ).toMatchSnapshot()
  replace(db, JEDI, { name: 'dead', age: 42 }, sql`where $age = 42`)
  expect(
    all<Jedi>(db, JEDI, sql`where $age = 42`).map(withoutID),
  ).toMatchSnapshot()
})

test('custom update age + 1', () => {
  const db = makeDB()
  expect(
    all<Jedi>(db, JEDI, sql`where $age = 42`).map(withoutID),
  ).toMatchSnapshot()
  const [query, args] = queryArgs(
    'update',
    JEDI,
    sql`set data = json_replace(data, '$.age', $age + 1)`,
  )
  db.query(query).run(...args)
  expect(
    all<Jedi>(db, JEDI, sql`where $age = 43`).map(withoutID),
  ).toMatchSnapshot()
})
