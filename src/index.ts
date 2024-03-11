import { Database } from 'bun:sqlite'
import { uuidv7 } from '@daaku/uuidv7'
import memize from 'memize'

export const qCreateTable = memize(
  (name: string): string => `create table if not exists ${name} (data text)`,
)

const exprToName = memize((expr: string): string =>
  expr.replace(/[^a-zA-Z]+/g, '_').replace(/_$/, ''),
)

export interface CreateIndex {
  table: string
  expr: string
  name?: string
  unique?: boolean
}
export const qCreateIndex = memize((o: CreateIndex): string => {
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

export const insert = <T extends Object>(
  db: Database,
  table: string,
  doc: T,
): T & { id: string } => {
  // @ts-expect-error muck with id
  if (!doc.id) {
    doc = { ...doc, id: uuidv7() }
  }
  const sql = qInsert(table)
  const stmt = db.query<undefined, string>(sql)
  stmt.run(JSON.stringify(doc))
  // @ts-expect-error muck with id
  return doc
}

export const $toData = memize((s: string): string =>
  s.replace(/\$([A-Za-z_][A-Za-z_\.]*)/g, `data->>'$1'`),
)

const $arrayToData = memize((vs: readonly string[]): string[] =>
  vs.map($toData),
)

export interface SQLParts {
  parts: readonly string[]
  values: any[]
}
export const sql = (
  strings: TemplateStringsArray,
  ...values: any[]
): SQLParts => {
  return {
    parts: $arrayToData(strings),
    values: values,
  }
}

export const queryArgs = (...sqls: (string | SQLParts)[]): [string, any[]] => {
  const query = [
    ...sqls.map(v => (typeof v === 'string' ? v : v.parts.join('?'))),
  ].join(' ')
  const args = sqls.flatMap(v => (typeof v === 'string' ? [] : v.values))
  return [query, args]
}

export const all = <Doc = unknown>(
  db: Database,
  table: string,
  ...sqls: SQLParts[]
): Doc[] => {
  const [query, args] = queryArgs('select data from', table, ...sqls)
  const stmt = db.query(query)
  // @ts-expect-error we expect [string][]
  return stmt.values(...args).flatMap(JSON.parse)
}

export const get = <Doc = unknown>(
  db: Database,
  table: string,
  ...sqls: SQLParts[]
): Doc | undefined => all<Doc>(db, table, ...sqls, sql`limit 1`)[0]

export const getByID = <Doc = unknown>(
  db: Database,
  table: string,
  id: string,
): Doc | undefined => get<Doc>(db, table, sql`where $id = ${id}`)

export const remove = (db: Database, table: string, ...sqls: SQLParts[]) => {
  const [query, args] = queryArgs('delete from', table, ...sqls)
  db.query(query).run(...args)
}

export const patch = <T extends Object>(
  db: Database,
  table: string,
  doc: T,
  ...sqls: SQLParts[]
) => {
  const [query, args] = queryArgs(
    'update',
    table,
    sql`set data = json_patch(data, ${JSON.stringify(doc)})`,
    ...sqls,
  )
  db.query(query).run(...args)
}

export const replace = <T extends Object>(
  db: Database,
  table: string,
  doc: T,
  ...sqls: SQLParts[]
) => {
  const [query, args] = queryArgs(
    'update',
    table,
    sql`set data = ${JSON.stringify(doc)}`,
    ...sqls,
  )
  db.query(query).run(...args)
}
