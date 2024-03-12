import { Database } from 'bun:sqlite'
import { uuidv7 } from '@daaku/uuidv7'
import memize from 'memize'

/**
 * SQL to create a table that stores JSON documents in a data column.
 */
export const qCreateTable = memize(
  (name: string): string => `create table if not exists ${name} (data blob)`,
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
  (table: string): string => `insert into ${table} (data) values (jsonb(?))`,
)

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

/**
 * Table provides access to a SQLite table storing JSON documents.
 */
export class Table<D extends Object> {
  #db: Database
  #table: string

  constructor(db: Database, table: string) {
    this.#db = db
    this.#table = table

    db.query(qCreateTable(table)).run()
    db.query(
      qCreateIndex({ table: table, unique: true, expr: $toData('$id') }),
    ).run()
  }

  get db(): Database {
    return this.#db
  }

  get table(): string {
    return this.#table
  }

  insert(doc: D): D & { id: string } {
    // @ts-expect-error muck with id
    if (!doc.id) {
      doc = { ...doc, id: uuidv7() }
    }
    const sql = qInsert(this.#table)
    const stmt = this.#db.query<undefined, string>(sql)
    stmt.run(JSON.stringify(doc))
    // @ts-expect-error muck with id
    return doc
  }

  all(...sqls: SQLParts[]): D[] {
    const [query, args] = queryArgs(
      'select json(data) from',
      this.#table,
      ...sqls,
    )
    const stmt = this.#db.query(query)
    // @ts-expect-error we expect [string][]
    return stmt.values(...args).flatMap(JSON.parse)
  }

  get(...sqls: SQLParts[]): D | undefined {
    return this.all(...sqls, sql`limit 1`)[0]
  }

  getByID(id: string): D | undefined {
    return this.get(sql`where $id = ${id}`)
  }

  delete(...sqls: SQLParts[]) {
    const [query, args] = queryArgs('delete from', this.#table, ...sqls)
    this.#db.query(query).run(...args)
  }

  patch(doc: Partial<D>, ...sqls: SQLParts[]) {
    const [query, args] = queryArgs(
      'update',
      this.#table,
      sql`set data = jsonb_patch(data, ${JSON.stringify(doc)})`,
      ...sqls,
    )
    this.#db.query(query).run(...args)
  }

  replace(doc: D, ...sqls: SQLParts[]) {
    const [query, args] = queryArgs(
      'update',
      this.#table,
      sql`set data = jsonb(${JSON.stringify(doc)})`,
      ...sqls,
    )
    this.#db.query(query).run(...args)
  }
}
