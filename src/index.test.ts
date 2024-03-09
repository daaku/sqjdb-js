import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import { uuidv7 } from '@daaku/uuidv7'

const expectIndexOp = (db: Database, sql: string, ...args: any[]) => {
  const explain = db.query(sql)
  expect(explain.all(...args).map((v: any) => v.opcode)).toContain('IdxGT')
}

test('crud', async () => {
  interface Jedi {
    id: string
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

  const qCreateTable = (name: string): string =>
    `create table if not exists ${name} (data text)`

  const exprToName = (expr: string): string =>
    expr.replace(/[^a-zA-Z]+/g, '_').replace(/_$/, '')

  const pathFor = (path: string | string[]): string =>
    `data->>'${Array.isArray(path) ? path.join('.') : path}'`

  interface CreateIndex {
    table: string
    expr: string
    name?: string
    unique?: boolean
  }
  const qCreateIndex = (o: CreateIndex): string => {
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
  }

  const cachedQCache = new Map<string, string>()
  const cachedQ = <Args extends any[]>(
    cb: (...args: Args) => string,
    ...args: Args
  ): string => {
    const keyS = cb.name + '_' + args.map(v => v.toString()).join('_')
    let sql = cachedQCache.get(keyS)
    if (!sql) {
      sql = cb(...args)
      cachedQCache.set(keyS, sql)
    }
    return sql
  }

  const qGetBy = (
    table: string,
    path: Parameters<typeof pathFor>[0] = 'id',
  ): string => `select data from ${table} where ${pathFor(path)} = ?`

  const getByID = <Doc = unknown>(
    db: Database,
    table: string,
    id: string,
  ): Doc | undefined => {
    const sql = cachedQ(qGetBy, table)
    const stmt = db.query<{ data: string }, string>(sql)
    const row = stmt.get(id)
    if (row) {
      return JSON.parse(row.data)
    }
  }

  const qInsert = (table: string): string =>
    `insert into ${table} (data) values (?)`

  const insert = <T extends Object>(db: Database, table: string, doc: T): T => {
    // @ts-ignore muck with id
    if (!doc.id) {
      doc = { ...doc, id: uuidv7() }
    }
    const sql = cachedQ(qInsert, table)
    const stmt = db.query<undefined, string>(sql)
    stmt.run(JSON.stringify(doc))
    return doc
  }

  const db = new Database(':memory:')
  const JEDI = 'jedi'
  db.query(qCreateTable(JEDI)).run()
  db.query(qCreateIndex({ table: JEDI, expr: pathFor('id') })).run()

  const yoda: Jedi = { id: uuidv7(), name: 'yoda', age: 900 }
  insert(db, JEDI, yoda)

  const fetchYoda = getByID<Jedi>(db, JEDI, yoda.id)
  expect(fetchYoda).toEqual(yoda)

  expectIndexOp(
    db,
    "explain select data from jedi where data->>'id' = ?",
    yoda.id,
  )
})
