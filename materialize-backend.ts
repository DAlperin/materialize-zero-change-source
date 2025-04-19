const HOST_ADDRESS = process.env.MATERIALIZE_HOST_ADDRESS;
const AUTH_OPTIONS = {
    user: process.env.MATERIALIZE_AUTH_OPTIONS_USER,
    password: process.env.MATERIALIZE_AUTH_OPTIONS_PASSWORD,
};
const DATABASE = process.env.MATERIALIZE_DATABASE;

import type { v0 } from "@rocicorp/zero/change-protocol/v0";
import { versionFromLexi } from "./zero-conn";

export class OutOfBoundsTimestampError extends Error {
    constructor(lastWatermark: string | undefined) {
        super(`Timestamp out of bounds: ${lastWatermark}`);
        this.name = "OutOfBoundsTimestampError";
    }
}

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

const createRequiredTablesQuery = `create table "zero.permissions" (permissions text, hash text);`

export default class MaterializeSocket {
    private socket: WebSocket | null = null;
    // This is the table or view we are subscribing to
    private collection: string;
    private _pending: any[] = [];
    private _progress = -Infinity;
    public staged: Map<string, Number> = new Map();
    public schema: TableSpec | null = null;
    public queryNumber = 0;
    private lastWatermark: bigint | undefined;
    private changeCallback: (collection: string) => void;
    private isConnected = false;

    constructor(collection: string, changeCallback: (collection: string) => void, lastWatermark: string | undefined = undefined) {
        console.log("MaterializeSocket constructor");
        this.collection = collection;
        this.changeCallback = changeCallback;

        if (lastWatermark) {
            this.lastWatermark = versionFromLexi(lastWatermark);
        }
    }

    /**
     * Connect to the Materialize WebSocket API
     * This method allows callers to catch errors during the connection process
     */
    public connect(): Promise<void> {
        return new Promise((resolve, reject) => {
            if (this.isConnected) {
                resolve();
                return;
            }

            try {
                this.socket = new WebSocket(`wss://${HOST_ADDRESS}/api/experimental/sql`);
                let query = `SUBSCRIBE TO (SELECT * FROM "${this.collection}") WITH (PROGRESS) ${this.lastWatermark ? `AS OF ${this.lastWatermark}` : ""}`;

                let newSchema: TableSpec = {
                    schema: "",
                    name: "",
                    columns: {},
                    primaryKey: []
                };

                // Handle connection errors
                this.socket.onerror = (error) => {
                    reject(new Error(`WebSocket connection error: ${error.toString()}`));
                };

                this.socket.onopen = () => {
                    if (!this.socket) return;
                    let queries = [
                        { query: createRequiredTablesQuery },
                        { query: schemaQuery(this.collection) },
                        { query: query }
                    ]
                    if (DATABASE) {
                        queries.unshift({ query: `SET DATABASE = "${DATABASE}"` });
                    }
                    this.socket.send(JSON.stringify(AUTH_OPTIONS));
                    this.socket.send(JSON.stringify({
                        queries: queries,
                    }));
                    // We don't resolve here because we need to wait for potential errors in the authentication/query setup
                };

                this.socket.onmessage = (ev) => {
                    let data = JSON.parse(ev.data);
                    if (data.type === "Error") {
                        let zero_perm_error_regex = /table ".+\.zero\.permissions" already exists/;
                        if (zero_perm_error_regex.test(data.payload.message)) {
                            // If we get here, the socket won't execute the rest of the commands
                            // so we need to send them again.
                            let queries = [
                                { query: schemaQuery(this.collection) },
                                { query: query }
                            ]
                            this.socket?.send(JSON.stringify({
                                queries: queries,
                            }));
                            this.queryNumber++;
                            // Ignore this error, it just means the table already exists
                            return;
                        }
                        console.error("MaterializeSocket error", data);
                        if (data.payload.message?.startsWith("Timestamp")) {
                            const error = new OutOfBoundsTimestampError(this.lastWatermark?.toString());
                            reject(error);
                            return;
                        }
                        const error = new Error(typeof data.payload === 'string' ? data.payload : JSON.stringify(data.payload));
                        reject(error);
                        return;
                    }

                    if (data.type === "CommandComplete") {
                        this.queryNumber++;
                        this.schema = newSchema;
                    }
                    // Schema query
                    if (this.queryNumber === 2) {
                        if (data.type === "Row") {
                            newSchema = this.buildSchema(data, newSchema);
                        }
                    } else if (this.queryNumber === 3) {
                        if (data.type === "Row") {
                            // Mark as connected once we actually start receiving data
                            if (!this.isConnected) {
                                this.isConnected = true;
                                resolve();
                            }
                        }
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
                            this.changeCallback(this.collection);
                        }
                    }
                };

                // Set a timeout to prevent hanging forever
                const timeout = setTimeout(() => {
                    if (!this.isConnected) {
                        reject(new Error("Connection timeout"));
                        this.close();
                    }
                }, 10000); // 10 second timeout

                // Clear the timeout once connected
                this.socket.addEventListener("open", () => {
                    clearTimeout(timeout);
                });
            } catch (error) {
                reject(error);
            }
        });
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
        if (this.socket) {
            this.socket.close();
            this.socket = null;
            this.isConnected = false;
        }
    }
}