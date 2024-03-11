import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import {
  $toData,
  all,
  get,
  getByID,
  insert,
  qCreateIndex,
  qCreateTable,
  sql,
} from './index.js'

const expectIndexOp = (db: Database, sql: string, ...args: any[]) => {
  const explain = db.query(sql)
  expect(explain.all(...args).map((v: any) => v.opcode)).toContain('IdxGT')
}

test('crud', async () => {
  interface Jedi {
    id?: string
    name: string
    age: number
    gender?: 'male' | 'female'
  }

  // update document
  // delete document

  const db = new Database(':memory:')
  const JEDI = 'jedi'
  db.query(qCreateTable(JEDI)).run()
  db.query(qCreateIndex({ table: JEDI, expr: $toData('$id') })).run()

  const yodaToInsert: Jedi = { name: 'yoda', age: 900, gender: 'male' }
  const yodaAsInserted = insert(db, JEDI, yodaToInsert)
  expect(yodaAsInserted).toMatchObject(yodaToInsert)
  expect(yodaAsInserted.id).toBeDefined()

  const yodaAsFetched = getByID<Jedi>(db, JEDI, yodaAsInserted.id)
  expect(yodaAsFetched).toEqual(yodaAsInserted)

  insert<Jedi>(db, JEDI, { name: 'luke', age: 42, gender: 'male' })
  insert<Jedi>(db, JEDI, { name: 'leia', age: 42, gender: 'female' })
  insert<Jedi>(db, JEDI, { name: 'grogu', age: 50, gender: 'male' })
  insert<Jedi>(db, JEDI, { name: 'rey', age: 32, gender: 'female' })

  expect($toData('$id')).toBe(`data->>'id'`)
  expect($toData('$id = 42')).toBe(`data->>'id' = 42`)
  expect($toData('$phone.home = 42')).toBe(`data->>'phone.home' = 42`)
  expect($toData('$phone_home = 42')).toBe(`data->>'phone_home' = 42`)
  expect($toData('$name = "foo" and $age = 42')).toBe(
    `data->>'name' = "foo" and data->>'age' = 42`,
  )

  expect(
    sql`$name = ${yodaAsInserted.name} and $age >= ${yodaAsInserted.age}`,
  ).toMatchSnapshot()

  expect(get<Jedi>(db, JEDI, sql`where $id = ${yodaAsInserted.id}`)?.name).toBe(
    yodaAsInserted.name,
  )
  expect(get<Jedi>(db, JEDI, sql`where $age = 50`)?.name).toBe('grogu')

  const byAgeAndGender = (
    db: Database,
    age: number,
    gender: 'male' | 'female',
  ): Jedi | undefined =>
    get<Jedi>(db, JEDI, sql`where $age = ${age} and $gender = ${gender}`)
  expect(byAgeAndGender(db, 42, 'male')?.name).toBe('luke')
  expect(byAgeAndGender(db, 42, 'female')?.name).toBe('leia')

  expect(
    all<Jedi>(db, JEDI, sql`where $age > ${42}`).map(j => j.name),
  ).toMatchSnapshot()

  expectIndexOp(
    db,
    "explain select data from jedi where data->>'id' = ?",
    yodaAsInserted.id,
  )
})
