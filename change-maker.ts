import { JSONValue } from "@rocicorp/zero";
import type { v0 } from "@rocicorp/zero/change-protocol/v0";

type TableSpec = v0.TableCreate['spec'];

export class ChangeMaker {
    makeInsertChanges(
        watermark: string,
        row: Record<string, JSONValue>,
        tableName: string,
        withTransaction = false
    ): v0.ChangeStreamMessage[] {
        const changes: v0.ChangeStreamMessage[] = [
            [
                'data',
                {
                    tag: 'insert',
                    new: row,
                    relation: {
                        name: tableName,
                        schema: 'public',
                        keyColumns: Object.keys(row)
                    }
                }
            ] satisfies v0.Data
        ];

        // if the change is already in a transaction, don't wrap it in another
        if (!withTransaction) {
            return changes;
        }

        return this.#wrapInTransaction(changes, watermark);
    }

    makeDeleteChanges(
        watermark: string,
        row: Record<string, string>,
        tableName: string,
        withTransaction = false
    ): v0.ChangeStreamMessage[] {
        const changes: v0.ChangeStreamMessage[] = [
            [
                'data',
                {
                    tag: 'delete',
                    key: row,
                    relation: {
                        name: tableName,
                        schema: 'public',
                        keyColumns: Object.keys(row)
                    }
                }
            ] satisfies v0.Data
        ];

        // if the change is already in a transaction, don't wrap it in another
        if (!withTransaction) {
            return changes;
        }

        return this.#wrapInTransaction(changes, watermark);
    }

    makeResetRequired(): v0.ChangeStreamMessage {
        return [
            'control',
            {
                tag: 'reset-required',
                message: "Materialize reset"
            }
        ]
    }

    makeCreateTableChanges(spec: TableSpec): v0.ChangeStreamMessage[] {
        if (!spec.primaryKey) {
            throw new Error(`Expected table ${spec.name} to have a primary key`);
        }

        const pkColSpec = spec.primaryKey.reduce(
            (acc, col) => {
                acc[col] = 'ASC';
                return acc;
            },
            {} as v0.IndexCreate['spec']['columns']
        );

        const changes: v0.ChangeStreamMessage[] = [
            // create the table
            [
                'data',
                {
                    tag: 'create-table',
                    spec
                } satisfies v0.TableCreate
            ],
            // and a unique index on the table's primary key columns
            [
                'data',
                {
                    tag: 'create-index',
                    spec: {
                        name: `idx_${spec.schema}__${spec.name}___id`,
                        schema: spec.schema,
                        tableName: spec.name,
                        columns: pkColSpec,
                        unique: true
                    }
                } satisfies v0.IndexCreate
            ]
        ];

        return changes;
    }

    makeZeroRequiredUpstreamTablesChanges(shardId: string): v0.ChangeStreamMessage[] {
        return [
            ...this.makeCreateTableChanges({
                schema: `public`,
                name: `zero_${shardId}.clients`,
                columns: {
                    clientGroupID: {
                        pos: 1,
                        dataType: 'text',
                        notNull: true
                    },
                    clientID: {
                        pos: 2,
                        dataType: 'text',
                        notNull: true
                    },
                    lastMutationID: {
                        pos: 3,
                        dataType: 'int8',
                        notNull: true
                    },
                    userID: {
                        pos: 4,
                        dataType: 'text'
                    }
                },
                primaryKey: ['clientGroupID', 'clientID']
            }),
            ...this.makeCreateTableChanges({
                schema: `public`,
                name: 'zero.schemaVersions',
                columns: {
                    minSupportedVersion: {
                        pos: 1,
                        dataType: 'int4'
                    },
                    maxSupportedVersion: {
                        pos: 2,
                        dataType: 'int4'
                    },
                    lock: {
                        pos: 3,
                        dataType: 'boolean',
                        notNull: true
                    }
                },
                primaryKey: ['lock']
            }),
        ];
    }

    makeBeginChanges(watermark: string): v0.ChangeStreamMessage[] {
        return [
            [
                'begin',
                { tag: 'begin' },
                { commitWatermark: watermark }
                //
            ] satisfies v0.Begin
        ];
    }

    makeCommitChanges(watermark: string): v0.ChangeStreamMessage[] {
        return [
            [
                'commit',
                { tag: 'commit' },
                { watermark }
                //
            ] satisfies v0.Commit
        ];
    }

    #wrapInTransaction(
        changes: Iterable<v0.ChangeStreamMessage>,
        watermark: string
    ): v0.ChangeStreamMessage[] {
        return [
            ...this.makeBeginChanges(watermark),
            ...changes,
            ...this.makeCommitChanges(watermark)
            //
        ];
    }

}
