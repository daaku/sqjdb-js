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

  const pathFor = memize(
    (path: string | string[]): string =>
      `data->>'${Array.isArray(path) ? path.join('.') : path}'`,
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

  const qGetBy = memize(
    (table: string, path: Parameters<typeof pathFor>[0] = 'id'): string =>
      `select data from ${table} where ${pathFor(path)} = ? limit 1`,
  )

  const getByID = <Doc = unknown>(
    db: Database,
    table: string,
    id: string,
  ): Doc | undefined => {
    const sql = qGetBy(table)
    const stmt = db.query<{ data: string }, string>(sql)
    const row = stmt.get(id)
    if (row) {
      return JSON.parse(row.data)
    }
  }

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

  const db = new Database(':memory:')
  const JEDI = 'jedi'
  db.query(qCreateTable(JEDI)).run()
  db.query(qCreateIndex({ table: JEDI, expr: pathFor('id') })).run()

  const yodaToInsert: Jedi = { name: 'yoda', age: 900 }
  const yodaAsInserted = insert(db, JEDI, yodaToInsert)
  expect(yodaAsInserted).toMatchObject(yodaToInsert)
  expect(yodaAsInserted.id).toBeDefined()

  const yodaAsFetched = getByID<Jedi>(db, JEDI, yodaAsInserted.id)
  expect(yodaAsFetched).toEqual(yodaAsInserted)

  expectIndexOp(
    db,
    "explain select data from jedi where data->>'id' = ?",
    yodaAsInserted.id,
  )
})
