import Foundation
import GRDB

actor KVStore {
    private let dbQueue: DatabaseQueue

    init(path: String) throws {
        dbQueue = try DatabaseQueue(path: path)
        try Self.makeMigrator().migrate(dbQueue)
    }

    private static func makeMigrator() -> DatabaseMigrator {
        var m = DatabaseMigrator()
        m.registerMigration("create_kv") { db in
            try db.create(table: "kv_records") { t in
                t.column("record_type", .text).notNull()
                t.column("k", .text).notNull()
                t.column("v", .blob).notNull()
                t.column("updated_at", .double).notNull()
                t.primaryKey(["record_type", "k"])
            }
            try db.create(index: "kv_by_type", on: "kv_records", columns: ["record_type"])
        }
        return m
    }

    func put(type: String, key: String, value: Data) async throws {
        try await dbQueue.write { db in
            try db.execute(
                sql: """
                INSERT INTO kv_records(record_type,k,v,updated_at)
                VALUES (?,?,?,?)
                ON CONFLICT(record_type,k)
                DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at
                """,
                arguments: [type, key, value, Date().timeIntervalSince1970]
            )
        }
    }

    func get(type: String, key: String) async throws -> Data? {
        try await dbQueue.read { db in
		if let row = try Row.fetchOne(db,
		    sql: "SELECT v FROM kv_records WHERE record_type=? AND k=?", arguments: [type, key]
		) {
    		return try row.withUnsafeData(named: "v") { data in
        		Data(data!)   // safe because column is NOT NULL
    		}
		}
		return nil
        }
    }

    func delete(type: String, key: String) async throws {
        try await dbQueue.write { db in
            _ = try db.execute(
                sql: "DELETE FROM kv_records WHERE record_type=? AND k=?",
                arguments: [type, key]
            )
        }
    }

    func exists(type: String, key: String) async throws -> Bool {
        try await dbQueue.read { db in
            try Bool.fetchOne(
                db,
                sql: "SELECT EXISTS(SELECT 1 FROM kv_records WHERE record_type=? AND k=?)",
                arguments: [type, key]
            ) ?? false
        }
    }

    func list(type: String, prefix: String?, limit: Int) async throws -> [String] {
        try await dbQueue.read { db in
            if let p = prefix, !p.isEmpty {
                return try String.fetchAll(
                    db,
                    sql: "SELECT k FROM kv_records WHERE record_type=? AND k LIKE ? ORDER BY k LIMIT ?",
                    arguments: [type, "\(p)%", limit]
                )
            } else {
                return try String.fetchAll(
                    db,
                    sql: "SELECT k FROM kv_records WHERE record_type=? ORDER BY k LIMIT ?",
                    arguments: [type, limit]
                )
            }
        }
    }
}