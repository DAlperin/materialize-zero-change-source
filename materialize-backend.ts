const HOST_ADDRESS = process.env.MATERIALIZE_HOST_ADDRESS;
const AUTH_OPTIONS = {
    user: process.env.MATERIALIZE_AUTH_OPTIONS_USER,
    password: process.env.MATERIALIZE_AUTH_OPTIONS_PASSWORD,
};

import type { v0 } from "@rocicorp/zero/change-protocol/v0";

type TableSpec = v0.TableCreate['spec'];

function schemaQuery(collection: string) {
    return `
SELECT 
  pc.oid::int8 AS "oid",
  nspname AS "schema", 
  pc.relname AS "name", 
  pc.relreplident AS "replicaIdentity",
  attnum AS "pos", 
  attname AS "col", 
  pt.typname AS "type", 
  atttypid::int8 AS "typeOID", 
  typtype,
  NULLIF(atttypmod, -1) AS "maxLen", 
  attnotnull AS "notNull",
  pg_get_expr(pd.adbin, pd.adrelid) as "dflt",
  NULLIF(ARRAY_POSITION(conkey, attnum), -1) AS "keyPos"
FROM pg_attribute
JOIN pg_class pc ON pc.oid = attrelid
JOIN pg_namespace pns ON pns.oid = relnamespace
JOIN pg_type pt ON atttypid = pt.oid
LEFT JOIN pg_constraint pk ON pk.contype = 'p' AND pk.connamespace = relnamespace AND pk.conrelid = attrelid
LEFT JOIN pg_attrdef pd ON pd.adrelid = attrelid AND pd.adnum = attnum
WHERE attgenerated = '' AND pc.relname = '${collection}' 
ORDER BY nspname, pc.relname;
`
}

export default class MaterializeSocket {
    private socket: WebSocket;
    // This is the table or view we are subscribing to
    private collection: string;
    private _pending: any[] = [];
    private _progress = -Infinity;
    public staged: Map<string, Number> = new Map();
    public schema: TableSpec | null = null;
    public queryNumber = 0;

    constructor(collection: string, changeCallback: (collection: string) => void) {
        console.log("MaterializeSocket constructor");
        this.collection = collection;
        this.socket = new WebSocket(`wss://${HOST_ADDRESS}/api/experimental/sql`);
        let query = `SUBSCRIBE TO (SELECT * FROM "${collection}") WITH (PROGRESS)`;
        this.socket.onopen = () => {
            this.socket.send(JSON.stringify(AUTH_OPTIONS));
            this.socket.send(JSON.stringify({
                queries: [
                    { query: schemaQuery(collection) },
                    { query: query }
                ]
            }));
        }

        let newSchema: TableSpec = {
            schema: "",
            name: "",
            columns: {},
            primaryKey: []
        };
        this.socket.onmessage = (ev) => {
            let data = JSON.parse(ev.data);
            if (data.type === "Error") {
                console.error("MaterializeSocket error", data);
                throw new Error(data.payload);
            }
            if (data.type === "CommandComplete") {
                this.queryNumber++;
                this.schema = newSchema;
            }
            // Schema query
            if (this.queryNumber === 0) {
                if (data.type === "Row") {
                    newSchema = this.buildSchema(data, newSchema);
                }
            } else if (this.queryNumber === 1) {
                // Parse log, and enqueue data.
                // Pend data updates; not progress statements.
                if (!data.payload[1]) {
                    this._pending.push(data);
                }
                // We can process anything with a timestamp strictly less than this.
                // Perhaps track the minimum timestamp and guard subsequent work by
                // a test that this timestamp is greater than it.
                if (this._progress < data.payload[0]) {
                    this._progress = data.payload[0];
                    changeCallback(this.collection);
                }
            }
        };
    }

    private buildSchema(data: any, schema: TableSpec) {
        // This should be typed way better.
        let col = data.payload;
        schema.schema = col[1];
        schema.name = col[2];
        schema.columns[col[5]] = {
            pos: Number(col[4]),
            dataType: col[6],
            notNull: col[10]
        };
        if (col[4] === '1') {
            schema.primaryKey?.push(col[5]);
        }
        return schema;
    }

    public get progress() {
        return this._progress;
    }

    public get pending() {
        return this._pending;
    }

    public set pending(pending: any[]) {
        this._pending = pending;
    }

    public close() {
        this.socket.close();
    }
} 