import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import { uuidv7 } from '@daaku/uuidv7'
import memize from 'memize'

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

  // create table
  // create indexes
  // insert document
  // one document
  // insert bulk
  // all document
  // update document
  // delete document

  const qCreateTable = memize(
    (name: string): string => `create table if not exists ${name} (data text)`,
  )

  const exprToName = memize((expr: string): string =>
    expr.replace(/[^a-zA-Z]+/g, '_').replace(/_$/, ''),
  )

  interface CreateIndex {
    table: string
    expr: string
    name?: string
    unique?: boolean
  }
  const qCreateIndex = memize((o: CreateIndex): string => {
    return [
      'create ',
      o.unique ? 'unique ' : '',
      'index if not exists ',
      o.name ?? `${o.table}_${exprToName(o.expr)}`,
      ' on ',
      o.table,
      ' (',
      o.expr,
      ')',
    ].join('')
  })

  const qInsert = memize(
    (table: string): string => `insert into ${table} (data) values (?)`,
  )

  const insert = <T extends Object>(
    db: Database,
    table: string,
    doc: T,
  ): T & { id: string } => {
    // @ts-ignore muck with id
    if (!doc.id) {
      doc = { ...doc, id: uuidv7() }
    }
    const sql = qInsert(table)
    const stmt = db.query<undefined, string>(sql)
    stmt.run(JSON.stringify(doc))
    // @ts-ignore muck with id
    return doc
  }

  const $toData = memize((s: string): string =>
    s.replace(/\$([A-Za-z_\.]+)/g, `data->>'$1'`),
  )

  const $arrayToData = memize((vs: readonly string[]): string[] =>
    vs.map($toData),
  )

  interface SQLParts {
    parts: readonly string[]
    values: any[]
  }
  const sql = (strings: TemplateStringsArray, ...values: any[]): SQLParts => {
    return {
      parts: $arrayToData(strings),
      values: values,
    }
  }

  const all = <Doc = unknown>(
    db: Database,
    table: string,
    ...sqls: SQLParts[]
  ): Doc[] => {
    const sql = [
      'select data from',
      table,
      ...sqls.map(v => v.parts.join('?')),
    ].join(' ')
    const args = sqls.flatMap(v => v.values)
    const stmt = db.query<{ data: string }, any[]>(sql)
    return stmt.all(...args).map(v => JSON.parse(v.data))
  }

  const get = <Doc = unknown>(
    db: Database,
    table: string,
    ...sqls: SQLParts[]
  ): Doc | undefined => all<Doc>(db, table, ...sqls)[0]

  const getByID = <Doc = unknown>(
    db: Database,
    table: string,
    id: string,
  ): Doc | undefined => get<Doc>(db, table, sql`where $id = ${id} limit 1`)

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
