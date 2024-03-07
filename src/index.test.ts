import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'

const opcodes = (vs: { opcode: string }[]): string[] => vs.map(v => v.opcode)

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

  const createTable = (name: string): string =>
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
  const createIndex = (o: CreateIndex): string => {
    return [
      'create ',
      o.unique ? 'unique ' : '',
      'index if not exists ',
      o.name ?? exprToName(o.expr),
      ' on ',
      o.table,
      ' (',
      o.expr,
      ')',
    ].join('')
  }

  const db = new Database(':memory:')
  db.query(createTable('jedi')).run()
  console.log(createIndex({ table: 'jedi', expr: pathFor('id') }))
  db.query(createIndex({ table: 'jedi', expr: pathFor('id') })).run()

  const yoda: Jedi = { id: 'yoda', name: 'yoda', age: 900 }

  const insert = db.query<undefined, string>(
    'insert into jedi (data) values (?)',
  )
  expect(insert.run(JSON.stringify(yoda)))

  const byID = db.query<{ data: string }, string>(
    "select data from jedi where data->>'id' = ?",
  )
  expect(JSON.parse(byID.get(yoda.id).data)).toEqual(yoda)

  const explainByID = db.query<{ data: string }, string>(
    "explain select data from jedi where data->>'id' = ?",
  )
  expect(opcodes(explainByID.all(yoda.id))).toContain('IdxGT')
})
